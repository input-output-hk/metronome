package io.iohk.metronome.hotstuff.service.execution

import cats.implicits._
import cats.data.{NonEmptyList, NonEmptyVector}
import cats.effect.{Sync, Concurrent, ContextShift, Resource}
import cats.effect.concurrent.Semaphore
import io.iohk.metronome.hotstuff.service.ApplicationService
import io.iohk.metronome.hotstuff.service.storage.{
  BlockStorage,
  ViewStateStorage
}
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  Block,
  Effect,
  QuorumCertificate
}
import io.iohk.metronome.hotstuff.service.tracing.ConsensusTracers
import io.iohk.metronome.storage.KVStoreRunner
import monix.catnap.ConcurrentQueue

/** The `BlockExecutor` receives ranges of committed blocks from the
  * `ConsensusService` and carries out their effects, marking the last
  * executed block in the `ViewStateStorage`, so that we can resume
  * from where we left off last time after a restart.
  *
  * It delegates other state updates to the `ApplicationService`.
  *
  * The `BlockExecutor` is prepared for gaps to appear in the ranges,
  * which happens if the node is out of sync with the federation and
  * needs to jump ahead.
  */
class BlockExecutor[F[_]: Sync, N, A <: Agreement: Block](
    appService: ApplicationService[F, A],
    blockStorage: BlockStorage[N, A],
    viewStateStorage: ViewStateStorage[N, A],
    executionQueue: ConcurrentQueue[F, Effect.ExecuteBlocks[A]],
    executionSemaphore: Semaphore[F]
)(implicit tracers: ConsensusTracers[F, A], storeRunner: KVStoreRunner[F, N]) {

  /** Add a newly committed batch of blocks to the execution queue. */
  def enqueue(effect: Effect.ExecuteBlocks[A]): F[Unit] =
    executionQueue.offer(effect)

  /** Fast forward state to a given block.
    *
    * This operation is delegated to the `BlockExecutor` so that it can make sure
    * that it's not executing other blocks at the same time.
    */
  def syncState(
      sources: NonEmptyVector[A#PKey],
      block: A#Block
  ): F[Unit] =
    executionSemaphore.withPermit {
      for {
        // Sync any application specific state, e.g. a ledger.
        // Do this before we prune the existing blocks and set the new root.
        canPrune <- appService.syncState(sources, block)
        // Prune the block store from earlier blocks that are no longer traversable.
        _ <- fastForwardStorage(block, canPrune)
      } yield ()
    }

  /** Execute blocks in order, updating pesistent storage along the way. */
  private def executeBlocks: F[Unit] = {
    executionQueue.poll
      .flatMap { case Effect.ExecuteBlocks(lastCommittedBlockHash, commitQC) =>
        // Retrieve the blocks from the storage from the last executed
        // to the one in the Quorum Certificate and tell the application
        // to execute them one by one. Update the persistent view state
        // after reach execution to remember which blocks we have truly
        // done.
        // Protect the whole thing with a semaphore from `syncState` being
        // carried out at the same time.
        executionSemaphore.withPermit {
          for {
            lastExecutedBlockHash <- getLastExecutedBlockHash
            blockHashes <- getBlockPath(
              lastExecutedBlockHash,
              lastCommittedBlockHash,
              commitQC
            )
            _ <- blockHashes match {
              case _ :: newBlockHashes =>
                tryExecuteBatch(newBlockHashes, commitQC, lastExecutedBlockHash)
              case Nil =>
                ().pure[F]
            }
          } yield ()
        }
      } >> executeBlocks
  }

  /** Read whatever was the last executed block that we peristed,
    * either by doing individual execution or state sync.
    */
  private def getLastExecutedBlockHash: F[A#Hash] =
    storeRunner.runReadOnly {
      viewStateStorage.getLastExecutedBlockHash
    }

  /** Update the last executed block hash, unless something else updated it
    * while we were executing blocks. This shouldn't happen if we used the
    * executor to carry out the state sync within the semaphore.
    */
  private def setLastExecutedBlockHash(
      blockHash: A#Hash,
      lastExecutedBlockHash: A#Hash
  ): F[Boolean] =
    storeRunner.runReadWrite {
      viewStateStorage
        .compareAndSetLastExecutedBlockHash(
          blockHash,
          lastExecutedBlockHash
        )
    }

  /** Get the more complete path. We may not have the last executed block any more.
    *
    * The first hash in the return value is a block that has already been executed.
    */
  private def getBlockPath(
      lastExecutedBlockHash: A#Hash,
      lastCommittedBlockHash: A#Hash,
      commitQC: QuorumCertificate[A]
  ): F[List[A#Hash]] = {
    def readPath(ancestorBlockHash: A#Hash) =
      storeRunner
        .runReadOnly {
          blockStorage.getPathFromAncestor(
            ancestorBlockHash,
            commitQC.blockHash
          )
        }

    readPath(lastExecutedBlockHash)
      .flatMap {
        case Nil =>
          readPath(lastCommittedBlockHash)
        case path =>
          path.pure[F]
      }
  }

  /** Try to execute a batch of newly committed blocks.
    *
    * The last executed block hash is used to track that it hasn't
    * been modified by the jump-ahead state sync mechanism while
    * we were executing blocks.
    *
    * In general we cannot expect to be able to cancel an ongoing execution,
    * it may be in the middle of carrying out some real-world effects that
    * don't support cancellation. We use the semaphore to protect against
    * race conditions between executing blocks here and the fast-forward
    * synchroniser making changes to state.
    */
  private def tryExecuteBatch(
      newBlockHashes: List[A#Hash],
      commitQC: QuorumCertificate[A],
      lastExecutedBlockHash: A#Hash
  ): F[Unit] = {
    def loop(
        newBlockHashes: List[A#Hash],
        lastExecutedBlockHash: A#Hash
    ): F[Unit] =
      newBlockHashes match {
        case Nil =>
          ().pure[F]

        case blockHash :: nextBlockHashes =>
          executeBlock(
            blockHash,
            commitQC,
            NonEmptyList(blockHash, nextBlockHashes),
            lastExecutedBlockHash
          ).attempt.flatMap {
            case Left(ex) =>
              // If a block fails, return what we managed to do so far,
              // so we can re-attempt it next time if the block is still
              // available in the storage.
              tracers
                .error(s"Error executing block $blockHash", ex)

            case Right(None) =>
              // Either the block couldn't be found, or the last executed
              // hash changed, but something suggests that we should not
              // try to execute more of this batch.
              nextBlockHashes.traverse(tracers.executionSkipped(_)).void

            case Right(Some(nextLastExecutedBlockHash)) =>
              loop(nextBlockHashes, nextLastExecutedBlockHash)
          }
      }

    loop(newBlockHashes, lastExecutedBlockHash)
  }

  /** Execute the next block in line and update the view state.
    *
    * The last executed block hash is only updated if the application
    * indicates that it has persisted the results, and if no other
    * changes have been made to it outside this loop. The execution
    * result carries the new last executed block hash to use in the
    * next iteration, or `None` if we should abandon the execution.
    */
  private def executeBlock(
      blockHash: A#Hash,
      commitQC: QuorumCertificate[A],
      commitPath: NonEmptyList[A#Hash],
      lastExecutedBlockHash: A#Hash
  ): F[Option[A#Hash]] = {
    assert(commitPath.head == blockHash)
    assert(commitPath.last == commitQC.blockHash)

    storeRunner.runReadOnly {
      blockStorage.get(blockHash)
    } flatMap {
      case None =>
        tracers.executionSkipped(blockHash).as(none)

      case Some(block) =>
        for {
          isPersisted <- appService.executeBlock(block, commitQC, commitPath)
          _           <- tracers.blockExecuted(blockHash)

          maybeLastExecutedBlockHash <-
            if (!isPersisted) {
              // Keep the last for the next compare and set below.
              lastExecutedBlockHash.some.pure[F]
            } else {
              // Check that nothing else changed the view state,
              // which should  be true as long as we use the semaphore.
              // Otherwise it would be up to the `ApplicationService` to
              // take care of isolation, and check that the block being
              // executed is the one we expected.
              setLastExecutedBlockHash(blockHash, lastExecutedBlockHash).map {
                case true  => blockHash.some
                case false => none
              }
            }
        } yield maybeLastExecutedBlockHash
    }
  }

  /** Replace the state we have persisted with what we synced with the federation.
    *
    * Prunes old blocks, the Commit Q.C. will be the new root.
    */
  private def fastForwardStorage(
      block: A#Block,
      canPrune: Boolean
  ): F[Unit] = {
    val blockHash = Block[A].blockHash(block)

    val prune = for {
      viewState <- viewStateStorage.getBundle.lift
      // Prune old data, but keep the new block.
      ds <- blockStorage
        .getDescendants(
          viewState.rootBlockHash,
          skip = Set(blockHash)
        )
        .lift
      _ <- ds.traverse(blockStorage.deleteUnsafe(_))
      _ <- viewStateStorage.setRootBlockHash(blockHash)
    } yield ()

    val query = for {
      // Insert the new block.
      _ <- blockStorage.put(block)
      _ <- prune.whenA(canPrune)
      // Considering the committed block as executed, we have its state already.
      _ <- viewStateStorage.setLastExecutedBlockHash(blockHash)
    } yield ()

    storeRunner.runReadWrite(query)
  }
}

object BlockExecutor {
  def apply[F[_]: Concurrent: ContextShift, N, A <: Agreement: Block](
      appService: ApplicationService[F, A],
      blockStorage: BlockStorage[N, A],
      viewStateStorage: ViewStateStorage[N, A]
  )(implicit
      tracers: ConsensusTracers[F, A],
      storeRunner: KVStoreRunner[F, N]
  ): Resource[F, BlockExecutor[F, N, A]] = for {
    executionQueue <- Resource.liftF {
      ConcurrentQueue[F].unbounded[Effect.ExecuteBlocks[A]](None)
    }
    executionSemaphore <- Resource.liftF(Semaphore[F](1))
    executor = new BlockExecutor[F, N, A](
      appService,
      blockStorage,
      viewStateStorage,
      executionQueue,
      executionSemaphore
    )
    _ <- Concurrent[F].background {
      executor.executeBlocks
    }
  } yield executor
}
