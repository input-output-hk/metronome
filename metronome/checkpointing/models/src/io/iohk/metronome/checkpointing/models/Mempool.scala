package io.iohk.metronome.checkpointing.models

import io.iohk.metronome.checkpointing.models.Transaction.ProposerBlock

case class Mempool(
    proposerBlocks: IndexedSeq[ProposerBlock],
    hasNewCheckpointCandidate: Boolean
) {
  def isEmpty: Boolean =
    proposerBlocks.isEmpty && !hasNewCheckpointCandidate
}

object Mempool {
  val empty: Mempool = Mempool(Vector.empty, false)
}
