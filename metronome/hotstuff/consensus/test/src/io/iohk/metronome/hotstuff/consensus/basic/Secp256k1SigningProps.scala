package io.iohk.metronome.hotstuff.consensus.basic

import cats.implicits._
import io.iohk.metronome.crypto
import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.crypto.{ECKeyPair, ECPublicKey}
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

object Secp256k1SigningProps extends Properties("Secp256k1Signing") {

  override def overrideParameters(p: Test.Parameters): Test.Parameters =
    p.withMinSuccessfulTests(10)

  object TestAgreement extends Secp256k1Agreement {
    type Block = Nothing
    type Hash  = crypto.hash.Hash
  }
  type TestAgreement = TestAgreement.type

  def serializer(
      phase: VotingPhase,
      viewNumber: ViewNumber,
      hash: crypto.hash.Hash
  ): ByteVector =
    ByteVector(phase.toString.getBytes) ++
      ByteVector.fromLong(viewNumber) ++
      hash

  def atLeast[A](n: Int, xs: Iterable[A]): Gen[Seq[A]] = {
    require(
      xs.size >= n,
      s"There has to be at least $n elements to choose from"
    )
    Gen.choose(n, xs.size).flatMap(Gen.pick(_, xs)).flatMap(_.toSeq)
  }

  val signing = Signing.secp256k1[TestAgreement](serializer)

  val keyPairs = List.fill(20)(ECKeyPair.generate(new SecureRandom))

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
    val Seq(signingKp, validationKp) = kps.toSeq

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
      kps <- Gen.atLeastOne(keyPairs)
      fed = buildFederation(kps)
      signers <- Gen.pick(fed.quorumSize, kps)
    } yield (fed, signers.map(_.prv)),
    arbitrary[ViewNumber],
    arbitrary[VotingPhase],
    arbitrary[Hash]
  ) { case ((federation, prvKeys), viewNumber, votingPhase, hash) =>
    val partialSigs =
      prvKeys.map(k => signing.sign(k, votingPhase, viewNumber, hash))
    val groupSig = signing.combine(partialSigs.toList)

    signing.validate(federation, groupSig, votingPhase, viewNumber, hash)
  }

  property("groupSignatureNonUniqueSigners") = forAll(
    for {
      kps <- atLeast(2, keyPairs)
      fed = buildFederation(kps)
      signers  <- Gen.pick(fed.quorumSize - 1, kps)
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
      kps <- Gen.atLeastOne(keyPairs) if kps.size < keyPairs.size
      fed = buildFederation(kps)
      signers <- Gen.pick(fed.quorumSize - 1, kps)
      foreign <- Gen.oneOf(keyPairs.diff(kps))
    } yield (fed, signers.map(_.prv), foreign.prv),
    arbitrary[ViewNumber],
    arbitrary[VotingPhase],
    arbitrary[Hash]
  ) { case ((federation, prvKeys, foreign), viewNumber, votingPhase, hash) =>
    val partialSigs =
      (foreign +: prvKeys).map(k =>
        signing.sign(k, votingPhase, viewNumber, hash)
      )
    val groupSig = signing.combine(partialSigs.toList)

    !signing.validate(federation, groupSig, votingPhase, viewNumber, hash)
  }

  property("groupSignatureNoQuorum") = forAll(
    for {
      kps <- Gen.atLeastOne(keyPairs)
      fed = buildFederation(kps)
      n       <- Gen.choose(0, kps.size) if n != fed.quorumSize
      signers <- Gen.pick(n, kps)
    } yield (signers.map(_.prv), fed),
    arbitrary[ViewNumber],
    arbitrary[VotingPhase],
    arbitrary[Hash]
  ) { case ((prvKeys, federation), viewNumber, votingPhase, hash) =>
    val partialSigs =
      prvKeys.map(k => signing.sign(k, votingPhase, viewNumber, hash))
    val groupSig = signing.combine(partialSigs.toList)

    !signing.validate(federation, groupSig, votingPhase, viewNumber, hash)
  }
}
