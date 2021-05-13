package io.iohk.metronome.hotstuff.service.codecs

import scodec.Codec
import scodec.codecs._
import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.hotstuff.service.messages.DuplexMessage

trait DefaultDuplexMessageCodecs[A <: Agreement, M] {
  self: DefaultMessageCodecs[A] =>

  implicit def applicationMessageCodec: Codec[M]

  implicit val duplexAgreementMessageCodec
      : Codec[DuplexMessage.AgreementMessage[A]] =
    Codec.deriveLabelledGeneric

  implicit val duplexApplicationMessageCodec
      : Codec[DuplexMessage.ApplicationMessage[M]] =
    Codec.deriveLabelledGeneric

  implicit val duplexMessageCodec: Codec[DuplexMessage[A, M]] =
    discriminated[DuplexMessage[A, M]]
      .by(uint4)
      .typecase(0, duplexAgreementMessageCodec)
      .typecase(1, duplexApplicationMessageCodec)
}
