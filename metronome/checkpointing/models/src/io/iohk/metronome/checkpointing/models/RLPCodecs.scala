package io.iohk.metronome.checkpointing.models

import cats.data.NonEmptyList
import io.iohk.ethereum.crypto.ECDSASignature
import io.iohk.ethereum.rlp.{RLPEncoder, RLPList}
import io.iohk.ethereum.rlp.RLPCodec
import io.iohk.ethereum.rlp.RLPCodec.Ops
import io.iohk.ethereum.rlp.RLPException
import io.iohk.ethereum.rlp.RLPImplicitDerivations._
import io.iohk.ethereum.rlp.RLPImplicits._
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.{
  Phase,
  VotingPhase,
  QuorumCertificate
}
import scodec.bits.{BitVector, ByteVector}

object RLPCodecs {
  implicit val rlpBitVector: RLPCodec[BitVector] =
    implicitly[RLPCodec[Array[Byte]]].xmap(BitVector(_), _.toByteArray)

  implicit val rlpByteVector: RLPCodec[ByteVector] =
    implicitly[RLPCodec[Array[Byte]]].xmap(ByteVector(_), _.toArray)

  implicit val hashRLPCodec: RLPCodec[Hash] =
    implicitly[RLPCodec[ByteVector]].xmap(Hash(_), identity)

  implicit val headerHashRLPCodec: RLPCodec[Block.Header.Hash] =
    implicitly[RLPCodec[ByteVector]].xmap(Block.Header.Hash(_), identity)

  implicit val bodyHashRLPCodec: RLPCodec[Block.Body.Hash] =
    implicitly[RLPCodec[ByteVector]].xmap(Block.Body.Hash(_), identity)

  implicit val ledgerHashRLPCodec: RLPCodec[Ledger.Hash] =
    implicitly[RLPCodec[ByteVector]].xmap(Ledger.Hash(_), identity)

  implicit val merkleHashRLPCodec: RLPCodec[MerkleTree.Hash] =
    implicitly[RLPCodec[ByteVector]].xmap(MerkleTree.Hash(_), identity)

  implicit val rlpProposerBlock: RLPCodec[Transaction.ProposerBlock] =
    deriveLabelledGenericRLPCodec

  implicit val rlpCheckpointCandidate
      : RLPCodec[Transaction.CheckpointCandidate] =
    deriveLabelledGenericRLPCodec

  implicit def rlpIndexedSeq[T: RLPCodec]: RLPCodec[IndexedSeq[T]] =
    seqEncDec[T]().xmap(_.toVector, _.toSeq)

  implicit def rlpNonEmptyList[T: RLPCodec]: RLPCodec[NonEmptyList[T]] =
    seqEncDec[T]().xmap(
      xs =>
        NonEmptyList.fromList(xs.toList).getOrElse {
          RLPException.decodeError("NonEmptyList", "List cannot be empty.")
        },
      _.toList
    )

  implicit val rlpLedger: RLPCodec[Ledger] =
    deriveLabelledGenericRLPCodec

  implicit val rlpTransaction: RLPCodec[Transaction] = {
    import Transaction._

    val ProposerBlockTag: Short       = 1
    val CheckpointCandidateTag: Short = 2

    def encodeWithTag[T: RLPEncoder](tag: Short, value: T) = {
      val t = RLPEncoder.encode(tag)
      val l = RLPEncoder.encode(value).asInstanceOf[RLPList]
      t +: l
    }

    RLPCodec.instance[Transaction](
      {
        case tx: ProposerBlock =>
          encodeWithTag(ProposerBlockTag, tx)
        case tx: CheckpointCandidate =>
          encodeWithTag(CheckpointCandidateTag, tx)
      },
      { case RLPList(tag, items @ _*) =>
        val rest = RLPList(items: _*)
        tag.decodeAs[Short]("tag") match {
          case ProposerBlockTag =>
            rest.decodeAs[ProposerBlock]("transaction")
          case CheckpointCandidateTag =>
            rest.decodeAs[CheckpointCandidate]("transaction")
          case unknown =>
            RLPException.decodeError(
              "Transaction",
              s"Unknown tag: $unknown",
              List(tag)
            )
        }
      }
    )
  }

  implicit val rlpBlockBody: RLPCodec[Block.Body] =
    deriveLabelledGenericRLPCodec

  implicit val rlpBlockHeader: RLPCodec[Block.Header] =
    deriveLabelledGenericRLPCodec

  // Cannot use derivation because Block is a sealed abstract case class,
  // so it doesn't allow creation of an invalid block.
  implicit val rlpBlock: RLPCodec[Block] =
    RLPCodec.instance[Block](
      block =>
        RLPList(
          RLPEncoder.encode(block.header),
          RLPEncoder.encode(block.body)
        ),
      { case RLPList(header, body) =>
        val h = header.decodeAs[Block.Header]("header")
        val b = body.decodeAs[Block.Body]("body")
        Block.makeUnsafe(h, b)
      }
    )

  implicit val rlpMerkleProof: RLPCodec[MerkleTree.Proof] =
    deriveLabelledGenericRLPCodec

  implicit val rlpViewNumber: RLPCodec[ViewNumber] =
    implicitly[RLPCodec[Long]].xmap(ViewNumber(_), identity)

  implicit val rlpVotingPhase: RLPCodec[VotingPhase] =
    RLPCodec.instance[VotingPhase](
      phase => {
        val tag: Short = phase match {
          case Phase.Prepare   => 1
          case Phase.PreCommit => 2
          case Phase.Commit    => 3
        }
        RLPEncoder.encode(tag)
      },
      { case tag =>
        tag.decodeAs[Short]("phase") match {
          case 1 => Phase.Prepare
          case 2 => Phase.PreCommit
          case 3 => Phase.Commit
          case u =>
            RLPException.decodeError(
              "phase",
              s"Unknown phase tag: $u",
              List(tag)
            )
        }
      }
    )

  implicit val rlpECDSASignature: RLPCodec[ECDSASignature] =
    RLPCodec.instance[ECDSASignature](
      sig => RLPEncoder.encode(sig.toBytes),
      { case enc =>
        val bytes = enc.decodeAs[ByteVector]("signature")
        ECDSASignature
          .fromBytes(akka.util.ByteString.fromArrayUnsafe(bytes.toArray))
          .getOrElse {
            RLPException.decodeError(
              "ECDSASignature",
              "Invalid signature format.",
              List(enc)
            )
          }
      }
    )

  implicit val rlpGroupSignature
      : RLPCodec[CheckpointingAgreement.GroupSignature] =
    deriveLabelledGenericRLPCodec

  // Derviation doesn't seem to work on generic case class.
  implicit val rlpQuorumCertificate
      : RLPCodec[QuorumCertificate[CheckpointingAgreement]] =
    RLPCodec.instance[QuorumCertificate[CheckpointingAgreement]](
      { case QuorumCertificate(phase, viewNumber, blockHash, signature) =>
        RLPList(
          RLPEncoder.encode(phase),
          RLPEncoder.encode(viewNumber),
          RLPEncoder.encode(blockHash),
          RLPEncoder.encode(signature)
        )
      },
      { case RLPList(phase, viewNumber, blockHash, signature) =>
        QuorumCertificate[CheckpointingAgreement](
          phase.decodeAs[VotingPhase]("phase"),
          viewNumber.decodeAs[ViewNumber]("viewNumber"),
          blockHash.decodeAs[CheckpointingAgreement.Hash]("blockHash"),
          signature.decodeAs[CheckpointingAgreement.GroupSignature]("signature")
        )
      }
    )

  implicit val rlpCheckpointCertificate: RLPCodec[CheckpointCertificate] =
    deriveLabelledGenericRLPCodec
}
