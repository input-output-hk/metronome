package io.iohk.metronome.checkpointing.service.models

import io.iohk.ethereum.rlp.RLPCodec
import io.iohk.ethereum.rlp.RLPCodec.Ops
import io.iohk.ethereum.rlp.RLPImplicitDerivations._
import io.iohk.ethereum.rlp.RLPImplicits._
import io.iohk.metronome.checkpointing.interpreter.models.Transaction
import scodec.bits.{BitVector, ByteVector}

object RLPCodecs {
  implicit val `RLPCodec[BitVector]` : RLPCodec[BitVector] =
    implicitly[RLPCodec[Array[Byte]]].xmap(BitVector(_), _.toByteArray)

  implicit val `RLPCodec[ByteVector]` : RLPCodec[ByteVector] =
    implicitly[RLPCodec[Array[Byte]]].xmap(ByteVector(_), _.toArray)

  implicit val `RLPCodec[Transaction.ProposerBlock]`
      : RLPCodec[Transaction.ProposerBlock] =
    deriveLabelledGenericRLPCodec

  implicit val `RLPCodec[Transaction.CheckpointCandidate]`
      : RLPCodec[Transaction.CheckpointCandidate] =
    deriveLabelledGenericRLPCodec

  implicit def `RLPCodec[Vector[T]]`[T: RLPCodec]: RLPCodec[Vector[T]] =
    seqEncDec[T]().xmap(_.toVector, _.toSeq)

  implicit val `RLPCodec[Ledger]` : RLPCodec[Ledger] =
    deriveLabelledGenericRLPCodec
}
