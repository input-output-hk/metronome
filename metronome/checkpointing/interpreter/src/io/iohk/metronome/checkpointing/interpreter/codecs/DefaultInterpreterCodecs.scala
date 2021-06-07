package io.iohk.metronome.checkpointing.interpreter.codecs

import io.iohk.ethereum.rlp
import io.iohk.ethereum.rlp.RLPCodec
import io.iohk.metronome.checkpointing.interpreter.messages.InterpreterMessage
import io.iohk.metronome.checkpointing.models.RLPCodecs
import scodec.{Codec, Attempt}
import scodec.bits.BitVector
import scala.util.Try

trait DefaultInterpreterCodecs {
  import scodec.codecs._
  import scodec.codecs.implicits._
  import InterpreterMessage._
  import RLPCodecs._

  // Piggybacking on `Codec[BitVector]` so that the length of each RLP
  // encoded field is properly carried over to the scodec derived data.
  private implicit def codecFromRLPCodec[T: RLPCodec]: Codec[T] =
    Codec[BitVector].exmap[T](
      (bits: BitVector) => {
        val tryDecode = Try(rlp.decode[T](bits.toByteArray))
        Attempt.fromTry(tryDecode)
      },
      (value: T) => {
        val bytes = rlp.encode(value)
        Attempt.successful(BitVector(bytes))
      }
    )

  private implicit def `Codec[Seq[T]]`[T: Codec]: Codec[Seq[T]] =
    Codec[List[T]].xmap(_.toSeq, _.toList)

  implicit val newProposerBlockRequestCodec: Codec[NewProposerBlockRequest] =
    Codec.deriveLabelledGeneric

  implicit val newCheckpointCandidateRequestCodec
      : Codec[NewCheckpointCandidateRequest] =
    Codec.deriveLabelledGeneric

  implicit val createBlockBodyRequestCodec: Codec[CreateBlockBodyRequest] =
    Codec.deriveLabelledGeneric

  implicit val createBlockBodyResponseCodec: Codec[CreateBlockBodyResponse] =
    Codec.deriveLabelledGeneric

  implicit val validateBlockBodyRequestCodec: Codec[ValidateBlockBodyRequest] =
    Codec.deriveLabelledGeneric

  implicit val validateBlockBodyResponseCodec
      : Codec[ValidateBlockBodyResponse] =
    Codec.deriveLabelledGeneric

  implicit val newCheckpointCertificateRequestCodec
      : Codec[NewCheckpointCertificateRequest] =
    Codec.deriveLabelledGeneric

  implicit val interpreterMessageCodec: Codec[InterpreterMessage] =
    discriminated[InterpreterMessage]
      .by(uint4)
      .typecase(0, newProposerBlockRequestCodec)
      .typecase(1, newCheckpointCandidateRequestCodec)
      .typecase(2, createBlockBodyRequestCodec)
      .typecase(3, createBlockBodyResponseCodec)
      .typecase(4, validateBlockBodyRequestCodec)
      .typecase(5, validateBlockBodyResponseCodec)
      .typecase(6, newCheckpointCertificateRequestCodec)
}

object DefaultInterpreterCodecs extends DefaultInterpreterCodecs
