package io.iohk.metronome.hotstuff.consensus

import io.iohk.metronome.core.Tagger
import io.iohk.metronome.hotstuff.consensus.ArbitraryInstances._
import org.scalacheck.Prop.forAll
import org.scalacheck._

abstract class LeaderSelectionProps(name: String, val selector: LeaderSelection)
    extends Properties(name) {

  object Size extends Tagger[Int]
  type Size = Size.Tagged

  implicit val arbFederationSize: Arbitrary[Size] = Arbitrary {
    Gen.posNum[Int].map(Size(_))
  }

  property("leaderOf") = forAll { (viewNumber: ViewNumber, size: Size) =>
    val idx = selector.leaderOf(viewNumber, size)
    0 <= idx && idx < size
  }
}

object RoundRobinSelectionProps
    extends LeaderSelectionProps(
      "LeaderSelection.RoundRobin",
      LeaderSelection.RoundRobin
    ) {

  property("round-robin") = forAll { (viewNumber: ViewNumber, size: Size) =>
    val idx0 = selector.leaderOf(viewNumber, size)
    val idx1 = selector.leaderOf(viewNumber.next, size)
    idx1 == idx0 + 1 || idx0 == size - 1 && idx1 == 0
  }
}

object HashingSelectionProps
    extends LeaderSelectionProps(
      "LeaderSelection.Hashing",
      LeaderSelection.Hashing
    )
