package io.iohk.metronome.hotstuff.service.execution

import cats.implicits._
import cats.effect.{Sync, Concurrent, ContextShift, Resource}
import io.iohk.metronome.hotstuff.service.ApplicationService
import io.iohk.metronome.hotstuff.service.storage.{
  BlockStorage,
  ViewStateStorage
}
import io.iohk.metronome.hotstuff.consensus.basic.{Agreement, Effect}
import monix.catnap.ConcurrentQueue

/** The `BlockExecutor` receives ranges of committed blocks from the
  * `ConsensusService` and carries out their effects, marking the last
  * executed block in the `ViewStateStorage`. It delegates other state
  * updates to the `ApplicationService`.
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
) {
  def enqueue(effect: Effect.ExecuteBlocks[A]): F[Unit] =
    executionQueue.offer(effect)

  /** Execute blocks in order, updating pesistent storage along the way. */
  private def executeBlocks: F[Unit] = {
    executionQueue.poll.flatMap {
      case Effect.ExecuteBlocks(lastExecutedBlockHash, commitQC) =>
        // Retrieve the blocks from the storage from the last executed
        // to the one in the Quorum Certificate and tell the application
        // to execute them one by one. Update the persistent view state
        // after reach execution to remember which blocks we have truly
        // done.

        // TODO (PM-3133): Execute block
        ???
    } >> executeBlocks
  }
}

object BlockExecutor {
  def apply[F[_]: Concurrent: ContextShift, N, A <: Agreement](
      appService: ApplicationService[F, A],
      blockStorage: BlockStorage[N, A],
      viewStateStorage: ViewStateStorage[N, A]
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
