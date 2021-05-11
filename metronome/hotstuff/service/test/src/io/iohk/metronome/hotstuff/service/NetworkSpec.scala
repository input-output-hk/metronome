package io.iohk.metronome.hotstuff.service

import cats.effect.Resource
import cats.effect.concurrent.Ref
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.networking.ConnectionHandler.MessageReceived
import monix.tail.Iterant

class NetworkSpec extends AsyncFlatSpec with Matchers {

  sealed trait TestMessage
  case class TestFoo(foo: String) extends TestMessage
  case class TestBar(bar: Int)    extends TestMessage

  object TestAgreement extends Agreement {
    override type Block = Nothing
    override type Hash  = Nothing
    override type PSig  = Nothing
    override type GSig  = Nothing
    override type PKey  = String
    override type SKey  = Nothing
  }
  type TestAgreement = TestAgreement.type

  type TestKeyAndMessage   = (TestAgreement.PKey, TestMessage)
  type TestMessageReceived = MessageReceived[TestAgreement.PKey, TestMessage]

  class TestNetwork(
      outbox: Vector[TestKeyAndMessage],
      val inbox: Ref[Task, Vector[
        MessageReceived[TestAgreement.PKey, TestMessage]
      ]]
  ) extends Network[Task, TestAgreement, TestMessage] {

    override def incomingMessages: Iterant[Task, TestMessageReceived] =
      Iterant.fromIndexedSeq {
        outbox.map { case (sender, message) =>
          MessageReceived(sender, message)
        }
      }

    override def sendMessage(
        recipient: TestAgreement.PKey,
        message: TestMessage
    ): Task[Unit] =
      inbox.update(_ :+ MessageReceived(recipient, message))
  }

  object TestNetwork {
    def apply(outbox: Vector[TestKeyAndMessage]) =
      Ref
        .of[Task, Vector[TestMessageReceived]](Vector.empty)
        .map(new TestNetwork(outbox, _))
  }

  behavior of "splitter"

  it should "split and merge messages" in {
    val messages = Vector(
      "Alice"   -> TestFoo("spam"),
      "Bob"     -> TestBar(42),
      "Charlie" -> TestFoo("eggs")
    )
    val resources = for {
      network <- Resource.liftF(TestNetwork(messages))
      (fooNetwork, barNetwork) <- Network
        .splitter[Task, TestAgreement, TestMessage, String, Int](network)(
          split = {
            case TestFoo(msg) => Left(msg)
            case TestBar(msg) => Right(msg)
          },
          merge = {
            case Left(msg)  => TestFoo(msg)
            case Right(msg) => TestBar(msg)
          }
        )
    } yield (network, fooNetwork, barNetwork)

    val test = resources.use { case (network, fooNetwork, barNetwork) =>
      for {
        fms <- fooNetwork.incomingMessages.take(2).toListL
        bms <- barNetwork.incomingMessages.take(1).toListL
        _   <- barNetwork.sendMessage("Dave", 123)
        _   <- fooNetwork.sendMessage("Eve", "Adam")
        _   <- barNetwork.sendMessage("Fred", 456)
        nms <- network.inbox.get
      } yield {
        fms shouldBe List(
          MessageReceived("Alice", "spam"),
          MessageReceived("Charlie", "eggs")
        )
        bms shouldBe List(MessageReceived("Bob", 42))
        nms shouldBe List(
          MessageReceived("Dave", TestBar(123)),
          MessageReceived("Eve", TestFoo("Adam")),
          MessageReceived("Fred", TestBar(456))
        )
      }
    }

    test.runToFuture
  }
}
