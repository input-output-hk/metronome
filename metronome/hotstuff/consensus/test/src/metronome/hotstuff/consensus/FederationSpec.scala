package metronome.hotstuff.consensus

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.Inside
import org.scalatest.prop.TableDrivenPropertyChecks._

class FederationSpec extends AnyFlatSpec with Matchers with Inside {

  implicit val ls = LeaderSelection.RoundRobin

  behavior of "Federation"

  it should "not create an empty federation" in {
    Federation(Vector.empty).isLeft shouldBe true
  }

  it should "not create a federation with duplicate keys" in {
    Federation(Vector(1, 2, 1)).isLeft shouldBe true
  }

  it should "not create a federation with too high configured f" in {
    Federation(1 to 4, maxFaulty = 2).isLeft shouldBe true
  }

  it should "determine the correct f and q based on n" in {
    val examples = Table(
      ("n", "f", "q"),
      (10, 3, 7),
      (1, 0, 1),
      (3, 0, 2),
      (4, 1, 3)
    )
    forAll(examples) { case (n, f, q) =>
      inside(Federation(1 to n)) { case Right(federation) =>
        federation.maxFaulty shouldBe f
        federation.quorumSize shouldBe q
      }
    }
  }

  it should "use lower quorum size if there are less faulties" in {
    val examples = Table(
      ("n", "f", "q"),
      (10, 2, 7),
      (10, 1, 6),
      (10, 0, 6),
      (9, 0, 5),
      (100, 0, 51),
      (100, 1, 51)
    )
    forAll(examples) { case (n, f, q) =>
      inside(Federation(1 to n, f)) { case Right(federation) =>
        federation.maxFaulty shouldBe f
        federation.quorumSize shouldBe q
      }
    }
  }

}
