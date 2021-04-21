package io.iohk.metronome.core.fibers

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.execution.atomic.AtomicInt
import org.scalatest.compatible.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.Inside
import scala.concurrent.duration._

class FiberSetSpec extends AsyncFlatSpec with Matchers with Inside {

  def test(t: Task[Assertion]) =
    t.timeout(10.seconds).runToFuture

  behavior of "FiberSet"

  it should "reject new submissions after shutdown" in test {
    FiberSet[Task].allocated.flatMap { case (fiberSet, release) =>
      for {
        _ <- fiberSet.submit(Task("foo"))
        _ <- release
        r <- fiberSet.submit(Task("bar")).attempt
      } yield {
        inside(r) { case Left(ex) =>
          ex shouldBe a[IllegalStateException]
          ex.getMessage should include("shut down")
        }
      }
    }
  }

  it should "cancel and raise errors in already submitted tasks after shutdown" in test {
    FiberSet[Task].allocated.flatMap { case (fiberSet, release) =>
      for {
        r <- fiberSet.submit(Task.never)
        _ <- release
        r <- r.attempt
      } yield {
        inside(r) { case Left(ex) =>
          ex shouldBe a[DeferredTask.CanceledException]
        }
      }
    }
  }

  it should "return a value we can wait on" in test {
    FiberSet[Task].use { fiberSet =>
      for {
        task  <- fiberSet.submit(Task("spam"))
        value <- task
      } yield {
        value shouldBe "spam"
      }
    }
  }

  it should "process tasks concurrently" in test {
    FiberSet[Task].use { fiberSet =>
      val running    = AtomicInt(0)
      val maxRunning = AtomicInt(0)

      for {
        handles <- Task.traverse(List.fill(10)(())) { _ =>
          val task = for {
            r <- Task(running.incrementAndGet())
            _ <- Task(maxRunning.getAndTransform(m => math.max(m, r)))
            _ <- Task.sleep(20.millis) // Increase chance for overlap.
            _ <- Task(running.decrement())
          } yield ()

          fiberSet.submit(task)
        }
        _ <- Task.parTraverse(handles)(identity)
      } yield {
        running.get() shouldBe 0
        maxRunning.get() should be > 1
      }
    }
  }
}
