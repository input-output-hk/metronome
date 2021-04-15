package io.iohk.metronome.core

import org.scalatest.flatspec.AsyncFlatSpec
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.matchers.should.Matchers

class PipeSpec extends AsyncFlatSpec with Matchers {

  behavior of "Pipe"

  it should "send messages between the sides" in {
    val test = for {
      pipe <- Pipe[Task, String, Int]
      _    <- pipe.left.send("foo")
      _    <- pipe.left.send("bar")
      _    <- pipe.right.send(1)
      rs   <- pipe.right.receive.take(2).toListL
      ls   <- pipe.left.receive.headOptionL
    } yield {
      rs shouldBe List("foo", "bar")
      ls shouldBe Some(1)
    }

    test.runToFuture
  }
}
