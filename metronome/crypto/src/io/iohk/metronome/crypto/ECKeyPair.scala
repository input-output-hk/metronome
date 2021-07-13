package io.iohk.metronome.crypto

import org.bouncycastle.crypto.AsymmetricCipherKeyPair

import java.security.SecureRandom

/** The pair of EC private and public keys for Secp256k1 elliptic curve */
case class ECKeyPair(prv: ECPrivateKey, pub: ECPublicKey) {

  /** The bouncycastle's underlying type for efficient use with
    * `io.iohk.ethereum.crypto.ECDSASignature`
    */
  def underlying: AsymmetricCipherKeyPair = prv.underlying
}

object ECKeyPair {

  def apply(keyPair: AsymmetricCipherKeyPair): ECKeyPair = {
    val (prv, pub) = io.iohk.ethereum.crypto.keyPairToByteArrays(keyPair)
    ECKeyPair(ECPrivateKey(prv), ECPublicKey(pub))
  }

  /** Generates a new keypair on the Secp256k1 elliptic curve */
  def generate(secureRandom: SecureRandom): ECKeyPair = {
    val kp = io.iohk.ethereum.crypto.generateKeyPair(secureRandom)
    ECKeyPair(kp)
  }
}
