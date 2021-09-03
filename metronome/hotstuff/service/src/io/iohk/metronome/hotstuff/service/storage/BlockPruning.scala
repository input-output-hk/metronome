package io.iohk.metronome.hotstuff.service.storage

import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.storage.KVStore

import cats.implicits._

object BlockPruning {

  def prune[N, A <: Agreement](
      blockStorage: BlockStorage[N, A],
      viewStateStorage: ViewStateStorage[N, A],
      blockHistorySize: Int
  ): KVStore[N, Unit] = {
    for {
      // Always keep the last executed block.
      lastExecutedBlock <- viewStateStorage.getLastExecutedBlockHash.lift
      pathFromRoot      <- blockStorage.getPathFromRoot(lastExecutedBlock).lift

      // Keep the last N blocks.
      pruneable = pathFromRoot.reverse
        .drop(blockHistorySize)
        .reverse

      // Make the last pruneable block the new root.
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
