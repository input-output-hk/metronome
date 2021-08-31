package io.iohk.metronome.checkpointing.models

import cats.data.NonEmptyList
import io.iohk.ethereum.crypto.ECDSASignature
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.hotstuff.consensus.basic.{
  Phase,
  QuorumCertificate,
  VotingPhase
}
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary
import scodec.bits.BitVector
import io.iohk.metronome.crypto.GroupSignature

object ArbitraryInstances
    extends io.iohk.metronome.hotstuff.consensus.ArbitraryInstances {
  implicit val arbBitVector: Arbitrary[BitVector] =
    Arbitrary {
      for {
        // Choose a size that BitVector still renders as hex in toString,
        // so the exact value is easy to see in test output or golden files.
        // Over that it renders hashCode which can differ between Scala versions.
        n  <- Gen.choose(0, 64)
        bs <- Gen.listOfN(n, arbitrary[Byte])
      } yield BitVector(bs.toArray)
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

  implicit val arbMempool: Arbitrary[Mempool] =
    Arbitrary {
      for {
        newCC <- arbitrary[Boolean]
        pbs   <- arbitrary[Set[Transaction.ProposerBlock]]
      } yield Mempool(pbs.toVector, newCC)
    }

  implicit val arbLedger: Arbitrary[Ledger] =
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
        height            <- Gen.posNum[Long]
        postStateHash     <- arbitrary[Ledger.Hash]
        transactions      <- arbitrary[Vector[Transaction]]
        contentMerkleRoot <- arbitrary[MerkleTree.Hash]
        body = Block.Body(transactions)
        header = Block.Header(
          parentHash,
          height,
          postStateHash,
          contentMerkleRoot
        )
      } yield Block.makeUnsafe(header, body)
    }

  implicit val arbETCBlock: Arbitrary[ETCBlock] =
    Arbitrary {
      for {
        parentHash   <- arbitrary[Hash]
        blockNumber  <- Gen.posNum[BigInt]
        hash         <- arbitrary[Hash]
        transactions <- arbitrary[Seq[Hash]]
        header = ETCBlock.Header(
          parentHash,
          hash,
          blockNumber
        )
        body = ETCBlock.Body(
          transactions
        )
      } yield ETCBlock.makeUnsafe(header, body)
    }

  implicit val arbBlockHeader: Arbitrary[Block.Header] =
    Arbitrary(arbitrary[Block].map(_.header))

  implicit val arbBlockBody: Arbitrary[Block.Body] =
    Arbitrary(arbitrary[Block].map(_.body))

  implicit val arETCBlockHeader: Arbitrary[ETCBlock.Header] = {
    Arbitrary(arbitrary[ETCBlock].map(_.header))
  }

  implicit val arETCBlockBody: Arbitrary[ETCBlock.Body] = {
    Arbitrary(arbitrary[ETCBlock].map(_.body))
  }

  implicit val arbECDSASignature: Arbitrary[ECDSASignature] =
    Arbitrary {
      for {
        r <- Gen.posNum[BigInt]
        s <- Gen.posNum[BigInt]
        v <- Gen.oneOf(
          ECDSASignature.positivePointSign,
          ECDSASignature.negativePointSign
        )
      } yield ECDSASignature(r, s, v)
    }

  implicit val arbQuorumCertificate
      : Arbitrary[QuorumCertificate[CheckpointingAgreement, VotingPhase]] =
    Arbitrary {
      for {
        phase      <- arbitrary[VotingPhase]
        viewNumber <- arbitrary[ViewNumber]
        blockHash  <- arbitrary[Block.Header.Hash]
        signature  <- arbitrary[CheckpointingAgreement.GSig]
      } yield QuorumCertificate[CheckpointingAgreement, VotingPhase](
        phase,
        viewNumber,
        blockHash,
        GroupSignature(signature)
      )
    }

  implicit val arbCheckpointCertificate: Arbitrary[CheckpointCertificate] =
    Arbitrary {
      for {
        n <- Gen.posNum[Int]
        headers <- Gen
          .listOfN(n, arbitrary[Block.Header])
          .map(NonEmptyList.fromListUnsafe(_))

        checkpoint <- arbitrary[Transaction.CheckpointCandidate]

        leafIndex <- Gen.choose(0, 10)
        siblings  <- arbitrary[Vector[MerkleTree.Hash]]
        proof = MerkleTree.Proof(leafIndex, siblings)

        viewNumber <- Gen.posNum[Long].map(x => ViewNumber(x + n))
        signature  <- arbitrary[CheckpointingAgreement.GSig]
        commitQC = QuorumCertificate[CheckpointingAgreement, Phase.Commit](
          phase = Phase.Commit,
          viewNumber = viewNumber,
          blockHash = headers.head.hash,
          signature = GroupSignature(signature)
        )

      } yield CheckpointCertificate(
        headers,
        checkpoint,
        proof,
        commitQC
      )
    }
}
