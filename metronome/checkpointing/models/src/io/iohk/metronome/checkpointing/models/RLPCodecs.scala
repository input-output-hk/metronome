package io.iohk.metronome.checkpointing.models

import io.iohk.ethereum.rlp.RLPCodec
import io.iohk.ethereum.rlp.RLPCodec.Ops
import io.iohk.ethereum.rlp.RLPImplicitDerivations._
import io.iohk.ethereum.rlp.RLPImplicits._
import io.iohk.ethereum.rlp.{RLPEncoder, RLPList}
import io.iohk.metronome.crypto.hash.Hash
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

  implicit val rlpProposerBlock: RLPCodec[Transaction.ProposerBlock] =
    deriveLabelledGenericRLPCodec

  implicit val rlpCheckpointCandidate
      : RLPCodec[Transaction.CheckpointCandidate] =
    deriveLabelledGenericRLPCodec

  implicit def rlpVector[T: RLPCodec]: RLPCodec[Vector[T]] =
    seqEncDec[T]().xmap(_.toVector, _.toSeq)

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
}
