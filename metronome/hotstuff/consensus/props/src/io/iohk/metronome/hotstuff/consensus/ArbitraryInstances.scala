package io.iohk.metronome.hotstuff.consensus

import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.hotstuff.consensus.basic.Phase.{
  Commit,
  PreCommit,
  Prepare
}
import io.iohk.metronome.hotstuff.consensus.basic.VotingPhase
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.rng.Seed
import org.scalacheck.{Arbitrary, Gen}
import scodec.bits.ByteVector

trait ArbitraryInstances {

  def sample[T: Arbitrary]: T = arbitrary[T].sample.get

  //TODO: rename / remove above?
  def sample0[T: Arbitrary](implicit seed: Seed): T =
    implicitly[Arbitrary[T]].arbitrary(Gen.Parameters.default, seed).get

  implicit val arbViewNumber: Arbitrary[ViewNumber] = Arbitrary {
    Gen.posNum[Long].map(ViewNumber(_))
  }

  implicit val arbVotingPhase: Arbitrary[VotingPhase] = Arbitrary {
    Gen.oneOf(Prepare, PreCommit, Commit)
  }

  implicit val arbHash: Arbitrary[Hash] =
    Arbitrary {
      Gen.listOfN(32, arbitrary[Byte]).map(ByteVector(_)).map(Hash(_))
    }
}

object ArbitraryInstances extends ArbitraryInstances
