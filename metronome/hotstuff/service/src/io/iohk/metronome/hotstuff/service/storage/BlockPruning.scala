package io.iohk.metronome.hotstuff.service.storage

import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.storage.KVStore

import cats.implicits._

object BlockPruning {

  /** Prune blocks which are not descendants of the N-th ancestor of the last executed block. */
  def prune[N, A <: Agreement](
      blockStorage: BlockStorage[N, A],
      viewStateStorage: ViewStateStorage[N, A],
      blockHistorySize: Int
  ): KVStore[N, Unit] = {
    for {
      // Always keep the last executed block.
      lastExecutedBlock <- viewStateStorage.getLastExecutedBlockHash.lift
      pathFromRoot      <- blockStorage.getPathFromRoot(lastExecutedBlock).lift

      // Everything but the last N blocks in the chain leading up to the
      // last executed block can be pruned.
      pruneable = pathFromRoot.dropRight(blockHistorySize)

      // Make the last pruneable block the new root. This gets rid of all
      // of its ancestors and the orphaned branches along the way.
      _ <- pruneable.lastOption match {
        case Some(newRoot) =>
          blockStorage.pruneNonDescendants(newRoot) >>
            viewStateStorage.setRootBlockHash(newRoot)

        case None =>
          KVStore.instance[N].unit
      }
    } yield ()
  }
}
