package io.iohk.metronome.checkpointing.models

import io.iohk.metronome.checkpointing.models.Transaction.ProposerBlock

case class Mempool(
    proposerBlocks: IndexedSeq[ProposerBlock],
    hasNewCheckpointCandidate: Boolean
) {
  def isEmpty: Boolean =
    proposerBlocks.isEmpty && !hasNewCheckpointCandidate

  def add(proposerBlock: ProposerBlock): Mempool =
    copy(proposerBlocks = proposerBlocks :+ proposerBlock)

  def add(proposerBlocks: Iterable[ProposerBlock]): Mempool =
    proposerBlocks.foldLeft(this)(_ add _)

  def removeProposerBlocks(toRemove: Seq[ProposerBlock]): Mempool =
    copy(proposerBlocks = proposerBlocks.diff(toRemove))

  def withNewCheckpointCandidate: Mempool =
    copy(hasNewCheckpointCandidate = true)

  def clearCheckpointCandidate: Mempool =
    copy(hasNewCheckpointCandidate = false)
}

object Mempool {

  /** Initial Mempool state
    *
    * Starting with `hasNewCheckpointCandidate` in case the PoW side
    * is unable to produce checkpoint notifications
    */
  val init: Mempool = Mempool(Vector.empty, true)
}
