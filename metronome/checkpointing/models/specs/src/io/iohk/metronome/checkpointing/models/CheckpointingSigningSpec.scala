package io.iohk.metronome.checkpointing.models

import io.iohk.metronome.crypto.ECKeyPair
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.hotstuff.consensus.basic.VotingPhase
import io.iohk.metronome.hotstuff.consensus.{
  Federation,
  LeaderSelection,
  ViewNumber
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.security.SecureRandom
import scodec.bits.ByteVector

/** Simple test cases to verify type interoperability.
  *
  * See [[io.iohk.metronome.hotstuff.consensus.basic.Secp256k1SigningProps]] for a more in-depth test
  */
class CheckpointingSigningSpec extends AnyFlatSpec with Matchers {
  import ArbitraryInstances._

  val keyPairs = IndexedSeq.fill(2)(ECKeyPair.generate(new SecureRandom))
  val federation = Federation(keyPairs.map(_.pub))(LeaderSelection.RoundRobin)
    .getOrElse(throw new Exception("Could not build federation"))

  "Checkpoint signing" should "work :)" in {
    val signing = new CheckpointingSigning(Block.Header.Hash(ByteVector.empty))

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

  it should "accept the genesis with no signatures" in {
    val genesisHash = sample[CheckpointingAgreement.Hash]
    val signing     = new CheckpointingSigning(genesisHash)

    val phase      = sample[VotingPhase]
    val viewNumber = sample[ViewNumber]
    val groupSig   = signing.combine(Nil)

    signing.validate(
      federation,
      groupSig,
      phase,
      viewNumber,
      genesisHash
    ) shouldBe true
  }

  it should "not accept the genesis with signatures" in {
    val genesisHash = sample[CheckpointingAgreement.Hash]
    val signing     = new CheckpointingSigning(genesisHash)

    val phase      = sample[VotingPhase]
    val viewNumber = sample[ViewNumber]

    val partialSigs =
      keyPairs.map(kp => signing.sign(kp.prv, phase, viewNumber, genesisHash))

    val groupSig = signing.combine(partialSigs)

    signing.validate(
      federation,
      groupSig,
      phase,
      viewNumber,
      genesisHash
    ) shouldBe false
  }
}
