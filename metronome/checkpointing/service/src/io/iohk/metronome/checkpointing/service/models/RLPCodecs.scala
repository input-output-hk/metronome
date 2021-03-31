package io.iohk.metronome.checkpointing.service.models

import io.iohk.ethereum.rlp.RLPCodec
import io.iohk.ethereum.rlp.RLPCodec.Ops
import io.iohk.ethereum.rlp.RLPImplicitDerivations._
import io.iohk.ethereum.rlp.RLPImplicits._
import io.iohk.metronome.checkpointing.interpreter.models.Transaction
import scodec.bits.{BitVector, ByteVector}

object RLPCodecs {
  implicit val rlpBitVector: RLPCodec[BitVector] =
    implicitly[RLPCodec[Array[Byte]]].xmap(BitVector(_), _.toByteArray)

  implicit val rlpByteVector: RLPCodec[ByteVector] =
    implicitly[RLPCodec[Array[Byte]]].xmap(ByteVector(_), _.toArray)

  implicit val rlpProposerBlock: RLPCodec[Transaction.ProposerBlock] =
    deriveLabelledGenericRLPCodec

  implicit val rlpCheckpointCandidate
      : RLPCodec[Transaction.CheckpointCandidate] =
    deriveLabelledGenericRLPCodec

  implicit def rlpVector[T: RLPCodec]: RLPCodec[Vector[T]] =
    seqEncDec[T]().xmap(_.toVector, _.toSeq)

  implicit val rlpLedger: RLPCodec[Ledger] =
    deriveLabelledGenericRLPCodec
}
