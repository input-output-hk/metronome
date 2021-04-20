package io.iohk.metronome.checkpointing

import io.iohk.metronome.crypto.ECKeyPair
import io.iohk.metronome.hotstuff.consensus.basic.{Signing, VotingPhase}
import io.iohk.metronome.hotstuff.consensus.{
  Federation,
  LeaderSelection,
  ViewNumber
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.security.SecureRandom

/** A single positive case spec to test type interoperability.
  * See [[io.iohk.metronome.hotstuff.consensus.basic.Secp256k1SigningProps]] for a more in-depth test
  */
class CheckpointSigningSpec extends AnyFlatSpec with Matchers {
  import models.ArbitraryInstances._

  "Checkpoint signing" should "work :)" in {
    val keyPairs = (1 to 2).map(_ => ECKeyPair.generate(new SecureRandom))
    val federation = Federation(keyPairs.map(_.pub))(LeaderSelection.RoundRobin)
      .getOrElse(throw new Exception("Could not build federation"))

    val signing: Signing[CheckpointingAgreement] =
      Signing.secp256k1(CheckpointingAgreement.signedContentSerialiser)

    val phase      = sample[VotingPhase]
    val viewNumber = sample[ViewNumber]
    val hash       = sample[CheckpointingAgreement.Hash]

    val partialSigs =
      keyPairs.map(kp => signing.sign(kp.prv, phase, viewNumber, hash))
    val groupSig = signing.combine(partialSigs)

    signing.validate(
      federation,
      groupSig,
      phase,
      viewNumber,
      hash
    ) shouldBe true
  }
}
