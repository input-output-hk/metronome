package io.iohk.metronome.hotstuff.consensus.basic

import cats.implicits._
import io.iohk.ethereum.crypto.ECDSASignature
import io.iohk.metronome.crypto
import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.crypto.{ECKeyPair, ECPrivateKey, ECPublicKey}
import io.iohk.metronome.hotstuff.consensus.ArbitraryInstances._
import io.iohk.metronome.hotstuff.consensus.{
  Federation,
  LeaderSelection,
  ViewNumber
}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.forAll
import org.scalacheck.{Gen, Properties, Test}
import scodec.bits.ByteVector

import java.security.SecureRandom

object Secp256k1SigningProps extends Properties("CheckpointingSigning") {

  override def overrideParameters(p: Test.Parameters): Test.Parameters =
    p.withMinSuccessfulTests(10)

  object TestAgreement extends Agreement {
    type Block = Nothing
    type Hash  = crypto.hash.Hash
    type PSig  = ECDSASignature
    type GSig  = List[ECDSASignature]
    type PKey  = ECPublicKey
    type SKey  = ECPrivateKey
  }
  type TestAgreement = TestAgreement.type

  def serialiser(
      phase: VotingPhase,
      viewNumber: ViewNumber,
      hash: crypto.hash.Hash
  ): ByteVector =
    ByteVector(phase.toString.getBytes) ++
      ByteVector.fromLong(viewNumber) ++
      hash

  val signing = Signing.secp256k1[TestAgreement](serialiser)

  val keyPairs = (1 to 20).toList.map(_ => ECKeyPair.generate(new SecureRandom))

  def buildFederation(kps: Iterable[ECKeyPair]): Federation[ECPublicKey] =
    Federation(kps.map(_.pub).toIndexedSeq)(
      LeaderSelection.RoundRobin
    ).valueOr(e => throw new Exception(s"Could not build Federation: $e"))

  property("partialSignatureCreation") = forAll(
    Gen.oneOf(keyPairs),
    arbitrary[ViewNumber],
    arbitrary[VotingPhase],
    arbitrary[Hash]
  ) { (keyPair, viewNumber, votingPhase, hash) =>
    val partialSig = signing.sign(keyPair.prv, votingPhase, viewNumber, hash)
    signing.validate(keyPair.pub, partialSig, votingPhase, viewNumber, hash)
  }

  property("noFalseValidation") = forAll(
    Gen.pick(2, keyPairs),
    arbitrary[ViewNumber],
    arbitrary[VotingPhase],
    arbitrary[Hash]
  ) { case (kps, viewNumber, votingPhase, hash) =>
    val Seq(signingKp, validationKp) = kps.toSeq //weird!

    val partialSig = signing.sign(signingKp.prv, votingPhase, viewNumber, hash)

    !signing.validate(
      validationKp.pub,
      partialSig,
      votingPhase,
      viewNumber,
      hash
    )
  }

  property("groupSignatureCreation") = forAll(
    for {
      kps     <- Gen.someOf(keyPairs) if kps.nonEmpty
      n       <- Gen.choose(kps.size - Federation.maxByzantine(kps.size), kps.size)
      signers <- Gen.pick(n, kps)
    } yield (kps, signers.map(_.prv)),
    arbitrary[ViewNumber],
    arbitrary[VotingPhase],
    arbitrary[Hash]
  ) { case ((kps, prvKeys), viewNumber, votingPhase, hash) =>
    val federation = buildFederation(kps)

    val partialSigs =
      prvKeys.map(k => signing.sign(k, votingPhase, viewNumber, hash))
    val groupSig = signing.combine(partialSigs.toList)

    signing.validate(federation, groupSig, votingPhase, viewNumber, hash)
  }

  property("groupSignatureNonUniqueSigners") = forAll(
    for {
      kps      <- Gen.someOf(keyPairs) if kps.nonEmpty
      n        <- Gen.choose(kps.size - Federation.maxByzantine(kps.size), kps.size)
      signers  <- Gen.pick(n, kps)
      repeated <- Gen.oneOf(signers)
    } yield (kps, signers.map(_.prv), repeated.prv),
    arbitrary[ViewNumber],
    arbitrary[VotingPhase],
    arbitrary[Hash]
  ) { case ((kps, prvKeys, repeated), viewNumber, votingPhase, hash) =>
    val federation = buildFederation(kps)

    val partialSigs =
      (repeated +: prvKeys).map(k =>
        signing.sign(k, votingPhase, viewNumber, hash)
      )
    val groupSig = signing.combine(partialSigs.toList)

    !signing.validate(federation, groupSig, votingPhase, viewNumber, hash)
  }

  property("groupSignatureForeignSigners") = forAll(
    for {
      kps     <- Gen.someOf(keyPairs) if kps.nonEmpty && kps.size < keyPairs.size
      n       <- Gen.choose(kps.size - Federation.maxByzantine(kps.size), kps.size)
      signers <- Gen.pick(n, kps)
      foreign <- Gen.oneOf(keyPairs.diff(kps))
    } yield (kps, signers.map(_.prv), foreign.prv),
    arbitrary[ViewNumber],
    arbitrary[VotingPhase],
    arbitrary[Hash]
  ) { case ((kps, prvKeys, foreign), viewNumber, votingPhase, hash) =>
    val federation = buildFederation(kps)

    val partialSigs =
      (foreign +: prvKeys).map(k =>
        signing.sign(k, votingPhase, viewNumber, hash)
      )
    val groupSig = signing.combine(partialSigs.toList)

    !signing.validate(federation, groupSig, votingPhase, viewNumber, hash)
  }

  property("groupSignatureNoQuorum") = forAll(
    for {
      kps     <- Gen.someOf(keyPairs) if kps.nonEmpty
      n       <- Gen.choose(0, math.max(0, Federation.maxByzantine(kps.size) - 1))
      signers <- Gen.pick(n, kps)
    } yield (kps, signers.map(_.prv)),
    arbitrary[ViewNumber],
    arbitrary[VotingPhase],
    arbitrary[Hash]
  ) { case ((kps, prvKeys), viewNumber, votingPhase, hash) =>
    val federation = buildFederation(kps)

    val partialSigs =
      prvKeys.map(k => signing.sign(k, votingPhase, viewNumber, hash))
    val groupSig = signing.combine(partialSigs.toList)

    !signing.validate(federation, groupSig, votingPhase, viewNumber, hash)
  }
}
