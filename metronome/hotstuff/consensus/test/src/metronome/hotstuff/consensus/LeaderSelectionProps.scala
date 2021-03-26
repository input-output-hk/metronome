package metronome.hotstuff.consensus

import metronome.core.Tagger
import org.scalacheck._
import org.scalacheck.Prop.forAll

class LeaderSelectionProps(name: String, selector: LeaderSelection)
    extends Properties(name) {

  object Size extends Tagger[Int]
  type Size = Size.Tagged

  implicit val arbViewNumber: Arbitrary[ViewNumber] = Arbitrary {
    Gen.posNum[Long].map(ViewNumber(_))
  }

  implicit val arbFederationSize: Arbitrary[Size] = Arbitrary {
    Gen.posNum[Int].map(Size(_))
  }

  property("leaderOf") = forAll { (viewNumber: ViewNumber, size: Size) =>
    val idx = selector.leaderOf(viewNumber, size)
    0 <= idx && idx < size
  }
}

object RoundRobinProps
    extends LeaderSelectionProps("RoundRobin", LeaderSelection.RoundRobin)
