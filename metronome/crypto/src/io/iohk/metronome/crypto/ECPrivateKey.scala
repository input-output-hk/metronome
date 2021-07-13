package io.iohk.metronome.crypto

import org.bouncycastle.crypto.AsymmetricCipherKeyPair
import scodec.bits.ByteVector
import io.iohk.ethereum.crypto.keyPairFromPrvKey

/** Wraps the bytes representing an EC private key */
case class ECPrivateKey(bytes: ByteVector) {
  require(
    bytes.length == ECPrivateKey.Length,
    s"Key must be ${ECPrivateKey.Length} bytes long"
  )

  /** Converts the byte representation to bouncycastle's `AsymmetricCipherKeyPair` for efficient use with
    * `io.iohk.ethereum.crypto.ECDSASignature`
    */
  val underlying: AsymmetricCipherKeyPair = keyPairFromPrvKey(
    bytes.toArray
  )
}

object ECPrivateKey {
  val Length = 32

  def apply(bytes: Array[Byte]): ECPrivateKey =
    ECPrivateKey(ByteVector(bytes))
}
