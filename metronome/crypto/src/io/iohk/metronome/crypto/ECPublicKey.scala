package io.iohk.metronome.crypto

import scodec.{Attempt, Codec}
import scodec.bits.ByteVector
import scodec.codecs.bytes
import scala.util.Try

/** Wraps the bytes representing an EC public key in uncompressed format and without the compression indicator */
case class ECPublicKey(bytes: ByteVector) {
  require(
    bytes.length == ECPublicKey.Length,
    s"Key must be ${ECPublicKey.Length} bytes long"
  )
}

object ECPublicKey {
  val Length = 64

  def apply(bytes: Array[Byte]): ECPublicKey =
    ECPublicKey(ByteVector(bytes))

  implicit val codec: Codec[ECPublicKey] =
    bytes.exmap[ECPublicKey](
      bytes => Attempt.fromTry(Try(ECPublicKey(bytes))),
      key => Attempt.successful(key.bytes)
    )
}
