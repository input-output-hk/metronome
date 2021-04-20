package io.iohk.metronome.crypto

import java.security.SecureRandom

object Secp256k1Utils {

  def generateKeyPair(
      secureRandom: SecureRandom
  ): ECKeyPair = {
    val kp = io.iohk.ethereum.crypto.generateKeyPair(secureRandom)
    ECKeyPair(kp)
  }

}
