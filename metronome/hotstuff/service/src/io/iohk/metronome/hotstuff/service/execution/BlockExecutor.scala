package io.iohk.metronome.hotstuff.service.execution

import cats.implicits._
import cats.data.NonEmptyList
import cats.effect.{Sync, Concurrent, ContextShift, Resource}
import io.iohk.metronome.hotstuff.service.ApplicationService
import io.iohk.metronome.hotstuff.service.storage.{
  BlockStorage,
  ViewStateStorage
}
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
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
class BlockExecutor[F[_]: Sync, N, A <: Agreement](
    appService: ApplicationService[F, A],
    blockStorage: BlockStorage[N, A],
    viewStateStorage: ViewStateStorage[N, A],
    executionQueue: ConcurrentQueue[F, Effect.ExecuteBlocks[A]]
)(implicit tracers: ConsensusTracers[F, A], storeRunner: KVStoreRunner[F, N]) {

  /** Add a newly committed batch of blocks to the execution queue. */
  def enqueue(effect: Effect.ExecuteBlocks[A]): F[Unit] =
    executionQueue.offer(effect)

  /** Execute blocks in order, updating pesistent storage along the way. */
  private def executeBlocks: F[Unit] = {
    executionQueue.poll
      .flatMap { case Effect.ExecuteBlocks(lastCommittedBlockHash, commitQC) =>
        // Retrieve the blocks from the storage from the last executed
        // to the one in the Quorum Certificate and tell the application
        // to execute them one by one. Update the persistent view state
        // after reach execution to remember which blocks we have truly
        // done.
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
      } >> executeBlocks
  }

  /** Read whatever was the last executed block that we persisted,
    * either here or by the fast-forward synchronizer.
    */
  private def getLastExecutedBlockHash: F[A#Hash] =
    storeRunner.runReadOnly {
      viewStateStorage.getLastExecutedBlockHash
    }

  /** Update the last executed block hash, unless the jump synchronizer did so
    * while we were executing blocks. NOTE: Would be good to stop executions
    * if that's happening, currently we expect that some blocks will just be
    * missing and the next batch will jump ahead. This is to avoid a race condition.
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
    * Return the last successfully executed hash, if any.
    */
  private def tryExecuteBatch(
      newBlockHashes: List[A#Hash],
      commitQC: QuorumCertificate[A],
      lastExecutedBlockHash: A#Hash
  ): F[A#Hash] = {
    def loop(
        newBlockHashes: List[A#Hash],
        lastExecutedBlockHash: A#Hash
    ): F[A#Hash] =
      newBlockHashes match {
        case Nil =>
          lastExecutedBlockHash.pure[F]

        case blockHash :: newBlockHashes =>
          executeBlock(
            blockHash,
            commitQC,
            NonEmptyList(blockHash, newBlockHashes),
            lastExecutedBlockHash
          ).attempt.flatMap {
            case Left(ex) =>
              // If a block fails, return what we managed to do so far,
              // so we can re-attempt it next time if the block is still
              // available in the storage.
              tracers
                .error(s"Error executing block $blockHash", ex)
                .as(lastExecutedBlockHash)

            case Right(()) =>
              loop(newBlockHashes, blockHash)
          }
      }

    loop(newBlockHashes, lastExecutedBlockHash)
  }

  /** Execute the next block in line and update the view state.
    * Be prepared that it may not exist, if execution took so long that
    * the `SyncService` skipped ahead to the latest Commit Q.C.
    */
  private def executeBlock(
      blockHash: A#Hash,
      commitQC: QuorumCertificate[A],
      commitPath: NonEmptyList[A#Hash],
      lastExecutedBlockHash: A#Hash
  ): F[Unit] = {
    assert(commitPath.head == blockHash)
    assert(commitPath.last == commitQC.blockHash)

    storeRunner.runReadOnly {
      blockStorage.get(blockHash)
    } flatMap {
      case None =>
        tracers.executionSkipped(blockHash)

      case Some(block) =>
        appService.executeBlock(block, commitQC, commitPath) >>
          setLastExecutedBlockHash(blockHash, lastExecutedBlockHash) >>
          tracers.blockExecuted(blockHash)
    }
  }
}

object BlockExecutor {
  def apply[F[_]: Concurrent: ContextShift, N, A <: Agreement](
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
    executor = new BlockExecutor[F, N, A](
      appService,
      blockStorage,
      viewStateStorage,
      executionQueue
    )
    _ <- Concurrent[F].background {
      executor.executeBlocks
    }
  } yield executor
}
