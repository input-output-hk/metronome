package io.iohk.metronome.hotstuff.service.codecs

import io.iohk.ethereum.crypto.ECDSASignature
import io.iohk.metronome.hotstuff.consensus.basic.{
  Secp256k1Agreement,
  Agreement
}
import scodec.{Codec, Attempt, Err}
import scodec.bits.BitVector

trait DefaultSecp256k1Codecs[A <: Secp256k1Agreement] {
  self: DefaultProtocolCodecs[A] =>
  import scodec.codecs.implicits._

  implicit val ecdsaSignatureCodec: Codec[ECDSASignature] = {
    import akka.util.ByteString
    Codec[BitVector].exmap(
      bits =>
        Attempt.fromOption(
          ECDSASignature.fromBytes(ByteString.fromArray(bits.toByteArray)),
          Err("Not a valid signature.")
        ),
      sig => Attempt.successful(BitVector(sig.toBytes.toArray[Byte]))
    )
  }

  override implicit def groupSignatureCodec
      : Codec[Agreement.GroupSignature[A]] =
    Codec.deriveLabelledGeneric

  override implicit def partialSignatureCodec
      : Codec[Agreement.PartialSignature[A]] =
    Codec.deriveLabelledGeneric

}
