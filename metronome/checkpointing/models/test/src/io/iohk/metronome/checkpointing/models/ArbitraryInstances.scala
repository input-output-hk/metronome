package io.iohk.metronome.checkpointing.models

import io.iohk.metronome.crypto.hash.Hash
import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary
import scodec.bits.BitVector
import scodec.bits.ByteVector

object ArbitraryInstances {
  implicit val arbBitVector: Arbitrary[BitVector] =
    Arbitrary {
      arbitrary[Array[Byte]].map(BitVector(_))
    }

  implicit val arbHash: Arbitrary[Hash] =
    Arbitrary {
      Gen.listOfN(32, arbitrary[Byte]).map(ByteVector(_)).map(Hash(_))
    }

  implicit val arbHeaderHash: Arbitrary[Block.Header.Hash] =
    Arbitrary(arbitrary[Hash].map(Block.Header.Hash(_)))

  implicit val arbBodyHash: Arbitrary[Block.Body.Hash] =
    Arbitrary(arbitrary[Hash].map(Block.Body.Hash(_)))

  implicit val arbLedgerHash: Arbitrary[Ledger.Hash] =
    Arbitrary(arbitrary[Hash].map(Ledger.Hash(_)))

  implicit val arbMerkleHash: Arbitrary[MerkleTree.Hash] =
    Arbitrary(arbitrary[Hash].map(MerkleTree.Hash(_)))

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

  implicit val arbBlock: Arbitrary[Block] =
    Arbitrary {
      for {
        parentHash        <- arbitrary[Block.Header.Hash]
        postStateHash     <- arbitrary[Ledger.Hash]
        transactions      <- arbitrary[Vector[Transaction]]
        contentMerkleRoot <- arbitrary[MerkleTree.Hash]
        body = Block.Body(transactions)
        header = Block.Header(
          parentHash,
          postStateHash,
          body.hash,
          contentMerkleRoot
        )
      } yield Block.makeUnsafe(header, body)
    }
}
