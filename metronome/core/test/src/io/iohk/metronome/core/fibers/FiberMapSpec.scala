package io.iohk.metronome.core.fibers

import cats.effect.concurrent.Ref
import monix.eval.Task
import monix.execution.atomic.AtomicInt
import monix.execution.Scheduler.Implicits.global
import org.scalatest.{Inspectors, Inside}
import org.scalatest.compatible.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.Random
import scala.concurrent.duration._
import monix.execution.BufferCapacity

class FiberMapsSpec extends AsyncFlatSpec with Matchers with Inside {

  behavior of "FiberMap"

  def test(t: Task[Assertion]) =
    t.runToFuture

  def testMap(f: FiberMap[Task, String] => Task[Assertion]) = test {
    FiberMap[Task, String]().use(f)
  }

  it should "process tasks in the order they are submitted" in testMap {
    fiberMap =>
      val stateRef = Ref.unsafe[Task, Map[String, Vector[Int]]](Map.empty)

      val keys = List("a", "b", "c")

      val valueMap = keys.map {
        _ -> Random.shuffle(Range(0, 10).toVector)
      }.toMap

      val tasks = for {
        k <- keys
        v <- valueMap(k)
      } yield (k, v)

      def append(k: String, v: Int): Task[Unit] =
        stateRef.update { state =>
          state.updated(k, state.getOrElse(k, Vector.empty) :+ v)
        }

      for {
        handles <- Task.traverse(tasks) { case (k, v) =>
          // This is a version that wouldn't preserve the order:
          // append(k, v).start.map(_.join)
          fiberMap.submit(k)(append(k, v))
        }
        _     <- Task.parTraverse(handles)(identity)
        state <- stateRef.get
      } yield {
        Inspectors.forAll(keys) { k =>
          state(k) shouldBe valueMap(k)
        }
      }
  }

  it should "process tasks concurrently across keys" in testMap { fiberMap =>
    val running    = AtomicInt(0)
    val maxRunning = AtomicInt(0)

    val keys  = List("a", "b")
    val tasks = List.fill(10)(keys).flatten

    for {
      handles <- Task.traverse(tasks) { k =>
        val task = for {
          r <- Task(running.incrementAndGet())
          _ <- Task(maxRunning.getAndTransform(m => math.max(m, r)))
          _ <- Task.sleep(20.millis) // Increase chance for overlap.
          _ <- Task(running.decrement())
        } yield ()

        fiberMap.submit(k)(task)
      }
      _ <- Task.parTraverse(handles)(identity)
    } yield {
      running.get() shouldBe 0
      maxRunning.get() shouldBe keys.size
    }
  }

  it should "return a value we can wait on" in testMap { fiberMap =>
    for {
      task  <- fiberMap.submit("foo")(Task("spam"))
      value <- task
    } yield {
      value shouldBe "spam"
    }
  }

  it should "reject new submissions after shutdown" in test {
    FiberMap[Task, String]().allocated.flatMap { case (fiberMap, release) =>
      for {
        _ <- fiberMap.submit("foo")(Task("alpha"))
        _ <- release
        r <- fiberMap.submit("foo")(Task(2)).attempt
      } yield {
        inside(r) { case Left(ex) =>
          ex shouldBe a[IllegalStateException]
          ex.getMessage should include("shut down")
        }
      }
    }
  }

  it should "reject new submissions for keys that hit their capacity limit" in test {
    FiberMap[Task, String](BufferCapacity.Bounded(capacity = 1)).use {
      fiberMap =>
        def trySubmit(k: String) =
          fiberMap.submit(k)(Task.never).attempt

        for {
          _  <- trySubmit("foo")
          _  <- trySubmit("foo")
          r3 <- trySubmit("foo")
          r4 <- trySubmit("bar")
        } yield {
          inside(r3) { case Left(ex) =>
            ex shouldBe a[FiberMap.QueueFullException]
          }
          r4.isRight shouldBe true
        }
    }
  }

  it should "cancel and raise errors in already submitted tasks after shutdown" in test {
    FiberMap[Task, String]().allocated.flatMap { case (fiberMap, release) =>
      for {
        r <- fiberMap.submit("foo")(Task.never)
        _ <- release
        r <- r.attempt
      } yield {
        inside(r) { case Left(ex) =>
          ex shouldBe a[RuntimeException]
          ex.getMessage should include("shut down")
        }
      }
    }
  }

  it should "keep processing even if a task fails" in testMap { fiberMap =>
    for {
      t1 <- fiberMap.submit("foo")(
        Task.raiseError(new RuntimeException("Boom!"))
      )
      t2 <- fiberMap.submit("foo")(Task(2))
      r1 <- t1.attempt
      r2 <- t2
    } yield {
      inside(r1) { case Left(ex) =>
        ex.getMessage shouldBe "Boom!"
      }
      r2 shouldBe 2
    }
  }
}
