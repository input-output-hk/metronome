package io.iohk.metronome.core.messages

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.compatible.Assertion
import org.scalatest.Inside
import scala.concurrent.Future
import scala.concurrent.duration._

class RPCTrackerSpec extends AsyncFlatSpec with Matchers with Inside {

  sealed trait TestMessage extends RPCMessage
  object TestMessage extends RPCMessageCompanion {
    case class FooRequest(requestId: RequestId) extends TestMessage with Request
    case class FooResponse(requestId: RequestId, value: Int)
        extends TestMessage
        with Response
    case class BarRequest(requestId: RequestId) extends TestMessage with Request
    case class BarResponse(requestId: RequestId, value: String)
        extends TestMessage
        with Response

    implicit val foo = pair[FooRequest, FooResponse]
    implicit val bar = pair[BarRequest, BarResponse]
  }
  import TestMessage._

  def test(
      f: RPCTracker[Task, TestMessage] => Task[Assertion]
  ): Future[Assertion] =
    RPCTracker[Task, TestMessage](10.seconds)
      .flatMap(f)
      .timeout(5.seconds)
      .runToFuture

  behavior of "RPCTracker"

  it should "complete responses within the timeout" in test { tracker =>
    val req = FooRequest(RequestId())
    val res = FooResponse(req.requestId, 1)
    for {
      join <- tracker.register(req)
      ok   <- tracker.complete(res)
      got  <- join
    } yield {
      ok shouldBe Right(true)
      got shouldBe Some(res)
    }
  }

  it should "complete responses with None after the timeout" in test {
    tracker =>
      val req = FooRequest(RequestId())
      val res = FooResponse(req.requestId, 1)
      for {
        join <- tracker.register(req, timeout = 50.millis)
        _    <- Task.sleep(100.millis)
        ok   <- tracker.complete(res)
        got  <- join
      } yield {
        ok shouldBe Right(false)
        got shouldBe empty
      }
  }

  it should "complete responses with None if the wrong type of response arrives" in test {
    tracker =>
      for {
        rid <- RequestId[Task]
        req = FooRequest(rid)
        res = BarResponse(rid, "one")
        join <- tracker.register(req)
        ok   <- tracker.complete(res)
        got  <- join
      } yield {
        inside(ok) { case Left(error) =>
          error.getMessage should include("Invalid response type")
        }
        got shouldBe empty
      }
  }

}
