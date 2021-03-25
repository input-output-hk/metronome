package metronome.crypto

import java.security.SecureRandom
import io.iohk.ethereum.crypto.generateKeyPair
import org.bouncycastle.crypto.AsymmetricCipherKeyPair
import org.bouncycastle.crypto.params.ECPublicKeyParameters
import scodec.bits.BitVector

object Secp256k1Utils {

  def generateSecp256k1KeyPair(
      secureRandom: SecureRandom
  ): AsymmetricCipherKeyPair = {
    generateKeyPair(secureRandom)
  }

  /** Returns secp256k1 public key bytes in uncompressed form
    */
  def secp256k1KeyPairToNodeId(keyPair: AsymmetricCipherKeyPair): BitVector = {
    BitVector(
      keyPair.getPublic
        .asInstanceOf[ECPublicKeyParameters]
        .getQ
        .getEncoded(false)
        .drop(1)
    )
  }

}
