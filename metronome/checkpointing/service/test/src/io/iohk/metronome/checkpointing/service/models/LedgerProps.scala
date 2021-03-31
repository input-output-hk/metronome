package io.iohk.metronome.checkpointing.service.models

import io.iohk.metronome.core.Validated
import io.iohk.metronome.checkpointing.interpreter.models.Transaction
import org.scalacheck._
import org.scalacheck.Prop.forAll

object LedgerProps extends Properties("Ledger") {
  import ArbitraryInstances._

  property("update") = forAll { (ledger: Ledger, transaction: Transaction) =>
    val updated = ledger.update(Validated[Transaction](transaction))
    transaction match {
      case _: Transaction.ProposerBlock =>
        (!ledger.proposerBlocks.contains(transaction) &&
          updated.proposerBlocks.last == transaction ||
          updated.proposerBlocks.contains(transaction)) &&
          updated.proposerBlocks.distinct == updated.proposerBlocks &&
          updated.maybeLastCheckpoint == ledger.maybeLastCheckpoint

      case _: Transaction.CheckpointCandidate =>
        updated.maybeLastCheckpoint == Some(transaction) &&
          updated.proposerBlocks.isEmpty
    }
  }

  property("hash") = forAll { (ledger1: Ledger, ledger2: Ledger) =>
    ledger1 == ledger2 && ledger1.hash == ledger2.hash ||
    ledger1 != ledger2 && ledger1.hash != ledger2.hash
  }
}