package io.iohk.metronome.checkpointing.service.models

import io.iohk.metronome.core.Validated
import io.iohk.metronome.checkpointing.interpreter.models.Transaction

/** Current state of the ledger after applying all previous blocks.
  *
  * Basically it's the last checkpoint, plus any accumulated proposer blocks
  * since then. Initially the last checkpoint is empty; conceptually it could
  * the the genesis block of the PoW chain, but we don't know what that is
  * until we talk to the interpreter, and we also can't produce it on our
  * own since it's opaque data.
  */
case class Ledger(
    maybeLastCheckpoint: Option[Transaction.CheckpointCandidate],
    proposerBlocks: Vector[Transaction.ProposerBlock]
) {

  /** Apply a validated transaction to produce the next ledger state.
    *
    * The transaction should have been validated against the PoW ledger
    * by this point, so we know for example that the new checkpoint is
    * a valid extension of the previous one.
    */
  def update(transaction: Validated[Transaction]): Ledger =
    (transaction: Transaction) match {
      case t @ Transaction.ProposerBlock(_) =>
        if (proposerBlocks.contains(t))
          this
        else
          copy(proposerBlocks = proposerBlocks :+ t)

      case t @ Transaction.CheckpointCandidate(_) =>
        Ledger(Some(t), Vector.empty)
    }
}

object Ledger {
  val empty = Ledger(None, Vector.empty)
}
