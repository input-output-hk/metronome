package io.iohk.metronome.networking

import cats.effect.Resource
import cats.effect.concurrent.Ref
import monix.eval.Task
import monix.tail.Iterant
import monix.execution.Scheduler.Implicits.global
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import io.iohk.metronome.networking.ConnectionHandler.MessageReceived

class NetworkSpec extends AsyncFlatSpec with Matchers {

  sealed trait TestMessage
  case class TestFoo(foo: String) extends TestMessage
  case class TestBar(bar: Int)    extends TestMessage

  type TestKey = String

  type TestKeyAndMessage   = (TestKey, TestMessage)
  type TestMessageReceived = MessageReceived[TestKey, TestMessage]

  class TestNetwork(
      outbox: Vector[TestKeyAndMessage],
      val inbox: Ref[Task, Vector[
        MessageReceived[TestKey, TestMessage]
      ]]
  ) extends Network[Task, TestKey, TestMessage] {

    override def incomingMessages: Iterant[Task, TestMessageReceived] =
      Iterant.fromIndexedSeq {
        outbox.map { case (sender, message) =>
          MessageReceived(sender, message)
        }
      }

    override def sendMessage(
        recipient: TestKey,
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
        .splitter[Task, TestKey, TestMessage, String, Int](network)(
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
