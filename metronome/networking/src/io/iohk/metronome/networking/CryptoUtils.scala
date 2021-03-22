package io.iohk.metronome.networking

import org.bouncycastle.asn1.sec.SECNamedCurves
import org.bouncycastle.asn1.x9.X9ECParameters
import org.bouncycastle.crypto.AsymmetricCipherKeyPair
import org.bouncycastle.crypto.generators.ECKeyPairGenerator
import org.bouncycastle.crypto.params.{
  ECDomainParameters,
  ECKeyGenerationParameters,
  ECPublicKeyParameters
}
import scodec.bits.BitVector

import java.security.SecureRandom

// TODO: Remove after publishing crypto stuff from mantis
object CryptoUtils {
  private val curveName = "secp256k1"

  private val curveParams: X9ECParameters = SECNamedCurves.getByName(curveName)

  private val curve: ECDomainParameters =
    new ECDomainParameters(
      curveParams.getCurve,
      curveParams.getG,
      curveParams.getN,
      curveParams.getH
    )

  def generateSecp256k1KeyPair(
      secureRandom: SecureRandom
  ): AsymmetricCipherKeyPair = {
    val generator = new ECKeyPairGenerator
    generator.init(new ECKeyGenerationParameters(curve, secureRandom))
    generator.generateKeyPair()
  }

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
