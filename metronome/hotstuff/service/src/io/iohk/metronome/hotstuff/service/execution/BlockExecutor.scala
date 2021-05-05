package io.iohk.metronome.hotstuff.service.execution

import cats.implicits._
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
    def loop(lastExecutedBlockHash: A#Hash): F[Unit] =
      executionQueue.poll
        .flatMap {
          case Effect.ExecuteBlocks(lastCommittedBlockHash, commitQC) =>
            // Retrieve the blocks from the storage from the last executed
            // to the one in the Quorum Certificate and tell the application
            // to execute them one by one. Update the persistent view state
            // after reach execution to remember which blocks we have truly
            // done.
            getBlockPath(
              lastExecutedBlockHash,
              lastCommittedBlockHash,
              commitQC
            ).flatMap {
              case _ :: newBlockHashes =>
                tryExecuteBatch(newBlockHashes)

              case Nil =>
                none[A#Hash].pure[F]
            }.map {
              _ getOrElse lastExecutedBlockHash
            }
        }
        .flatMap(loop)

    storeRunner
      .runReadOnly {
        viewStateStorage.getBundle.map(_.lastExecutedBlockHash)
      }
      .flatMap(loop)
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
            lastExecutedBlockHash,
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
      newBlockHashes: List[A#Hash]
  ): F[Option[A#Hash]] = {
    def loop(
        newBlockHashes: List[A#Hash],
        lastExecutedBlockHash: Option[A#Hash]
    ): F[Option[A#Hash]] =
      newBlockHashes match {
        case Nil =>
          lastExecutedBlockHash.pure[F]

        case blockHash :: newBlockHashes =>
          executeBlock(blockHash).attempt.flatMap {
            case Left(ex) =>
              // If a block fails, return what we managed to do so far,
              // so we can re-attempt it next time if the block is still
              // available in the storage.
              tracers
                .error(s"Error executiong block $blockHash", ex)
                .as(lastExecutedBlockHash)

            case Right(()) =>
              loop(newBlockHashes, blockHash.some)
          }
      }

    loop(newBlockHashes, none)
  }

  /** Execute the next block in line and update the view state.
    * Be prepared that it may not exist, if execution took so long that
    * the `SyncService` skipped ahead to the latest Commit Q.C.
    */
  private def executeBlock(blockHash: A#Hash): F[Unit] = {
    storeRunner.runReadOnly {
      blockStorage.get(blockHash)
    } flatMap {
      case None =>
        tracers.executionSkipped(blockHash)

      case Some(block) =>
        for {
          _ <- appService.executeBlock(block)
          _ <- storeRunner.runReadWrite {
            viewStateStorage.setLastExecutedBlockHash(blockHash)
          }
          _ <- tracers.blockExecuted(blockHash)
        } yield ()
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
