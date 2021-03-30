package io.iohk.metronome.crypto

import java.security.SecureRandom
import org.bouncycastle.crypto.AsymmetricCipherKeyPair
import org.bouncycastle.crypto.params.ECPublicKeyParameters
import scodec.bits.BitVector

object Secp256k1Utils {

  def generateKeyPair(
      secureRandom: SecureRandom
  ): AsymmetricCipherKeyPair = {
    io.iohk.ethereum.crypto.generateKeyPair(secureRandom)
  }

  /** Returns secp256k1 public key bytes in uncompressed form, with compression indicator stripped
    */
  def keyPairToUncompressed(keyPair: AsymmetricCipherKeyPair): BitVector = {
    BitVector(
      keyPair.getPublic
        .asInstanceOf[ECPublicKeyParameters]
        .getQ
        .getEncoded(false)
        .drop(1)
    )
  }

}
