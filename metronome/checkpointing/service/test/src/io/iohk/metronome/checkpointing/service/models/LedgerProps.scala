package io.iohk.metronome.checkpointing.service.models

import io.iohk.metronome.core.Validated
import io.iohk.metronome.checkpointing.interpreter.models.Transaction
import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.forAll
import scodec.bits.BitVector

object LedgerProps extends Properties("Ledger") {

  implicit val arbBitVector = Arbitrary {
    arbitrary[Array[Byte]].map(BitVector(_))
  }

  implicit val arbProposerBlock = Arbitrary {
    arbitrary[BitVector].map(Transaction.ProposerBlock(_))
  }

  implicit val arbCheckpointCandidate = Arbitrary {
    arbitrary[BitVector].map(Transaction.CheckpointCandidate(_))
  }

  implicit val arbTransaction: Arbitrary[Transaction] = Arbitrary {
    Gen.frequency(
      4 -> arbitrary[Transaction.ProposerBlock],
      1 -> arbitrary[Transaction.CheckpointCandidate]
    )
  }

  implicit val arbLeger = Arbitrary {
    for {
      mcp <- arbitrary[Option[Transaction.CheckpointCandidate]]
      pbs <- arbitrary[Set[Transaction.ProposerBlock]].map(_.toVector)
    } yield Ledger(mcp, pbs)
  }

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
}
