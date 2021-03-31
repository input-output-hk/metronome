package io.iohk.metronome.checkpointing.service.models

import io.iohk.metronome.checkpointing.interpreter.models.Transaction
import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary
import scodec.bits.BitVector

object ArbitraryInstances {
  implicit val arbBitVector: Arbitrary[BitVector] =
    Arbitrary {
      arbitrary[Array[Byte]].map(BitVector(_))
    }

  implicit val arbProposerBlock: Arbitrary[Transaction.ProposerBlock] =
    Arbitrary {
      arbitrary[BitVector].map(Transaction.ProposerBlock(_))
    }

  implicit val arbCheckpointCandidate
      : Arbitrary[Transaction.CheckpointCandidate] =
    Arbitrary {
      arbitrary[BitVector].map(Transaction.CheckpointCandidate(_))
    }

  implicit val arbTransaction: Arbitrary[Transaction] =
    Arbitrary {
      Gen.frequency(
        4 -> arbitrary[Transaction.ProposerBlock],
        1 -> arbitrary[Transaction.CheckpointCandidate]
      )
    }

  implicit val arbLeger: Arbitrary[Ledger] =
    Arbitrary {
      for {
        mcp <- arbitrary[Option[Transaction.CheckpointCandidate]]
        pbs <- arbitrary[Set[Transaction.ProposerBlock]].map(_.toVector)
      } yield Ledger(mcp, pbs)
    }
}
