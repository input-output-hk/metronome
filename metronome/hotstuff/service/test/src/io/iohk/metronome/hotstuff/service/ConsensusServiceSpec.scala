package io.iohk.metronome.hotstuff.service

import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.compatible.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers
import io.iohk.metronome.hotstuff.consensus.basic.{
  ProtocolError,
  Event,
  Message,
  Phase,
  QuorumCertificate
}
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.crypto.GroupSignature

class ConsensusServiceSpec extends AsyncFlatSpec with Matchers {
  import ConsensusService.MessageStash

  def testT[T](t: Task[Assertion]) =
    t.timeout(10.seconds).runToFuture

  def test(t: => Assertion) =
    testT(Task(t))

  object TestAgreement extends Agreement {
    override type Block = Nothing
    override type Hash  = Int
    override type PSig  = Nothing
    override type GSig  = Int
    override type PKey  = String
    override type SKey  = Nothing
  }
  type TestAgreement = TestAgreement.type

  "MessageStash" should behave like {

    val emptyStash = MessageStash.empty[TestAgreement]

    val error = ProtocolError.TooEarly[TestAgreement](
      Event.MessageReceived[TestAgreement](
        "Alice",
        Message.NewView(
          ViewNumber(10),
          QuorumCertificate[TestAgreement](
            Phase.Prepare,
            ViewNumber(9),
            123,
            GroupSignature(456)
          )
        )
      ),
      expectedInViewNumber = ViewNumber(11),
      expectedInPhase = Phase.Prepare
    )
    val errorSlotKey = (error.expectedInViewNumber, error.expectedInPhase)

    it should "stash errors" in test {
      emptyStash.slots shouldBe empty

      val stash = emptyStash.stash(error)

      stash.slots should contain key errorSlotKey
      stash.slots(errorSlotKey) should contain key error.event.sender
      stash.slots(errorSlotKey)(error.event.sender) shouldBe error.event.message
    }

    it should "stash only the last message from a sender" in test {
      val error2 = error.copy(event =
        error.event.copy(message =
          Message.NewView(
            ViewNumber(10),
            QuorumCertificate[TestAgreement](
              Phase.Prepare,
              ViewNumber(8),
              122,
              GroupSignature(455)
            )
          )
        )
      )
      val stash = emptyStash.stash(error).stash(error2)

      stash.slots(errorSlotKey)(
        error.event.sender
      ) shouldBe error2.event.message
    }

    it should "unstash due errors" in test {
      val errors = List(
        error,
        error.copy(
          expectedInPhase = Phase.PreCommit
        ),
        error.copy(
          expectedInViewNumber = error.expectedInViewNumber.next
        ),
        error.copy(
          expectedInViewNumber = error.expectedInViewNumber.next,
          expectedInPhase = Phase.Commit
        ),
        error.copy(
          expectedInViewNumber = error.expectedInViewNumber.next.next
        )
      )

      val stash0 = errors.foldLeft(emptyStash)(_ stash _)

      val (stash1, unstashed) = stash0.unstash(
        errors(2).expectedInViewNumber,
        errors(2).expectedInPhase
      )

      stash1.slots.keySet should have size 2
      unstashed should have size 3
    }
  }
}
