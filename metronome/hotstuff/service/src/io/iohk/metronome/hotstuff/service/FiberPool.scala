package io.iohk.metronome.hotstuff.service

import cats.implicits._
import cats.effect.{Sync, Concurrent, ContextShift, Fiber, Resource}
import cats.effect.concurrent.{Ref, Semaphore, Deferred}
import monix.catnap.ConcurrentQueue

/** Execute tasks on a separate fiber per source public key,
  * facilitating separate rate limiting and fair concurrency.
  */
class FiberPool[F[_]: Concurrent: ContextShift, PKey](
    isShutdownRef: Ref[F, Boolean],
    actorMapRef: Ref[F, Map[PKey, FiberPool.Actor[F]]],
    semaphore: Semaphore[F]
) {

  /** Submit a task to be processed in the background.
    *
    * Create a new fiber if the given key hasn't got one yet.
    *
    * The result can be waited upon or discarded, the processing
    * will happen in the background regardless.
    */
  def submit[A](key: PKey)(task: F[A]): F[F[A]] = {
    isShutdownRef.get.flatMap {
      case true =>
        Sync[F].raiseError(
          new IllegalStateException("The pool is already shut down.")
        )
      case false =>
        actorMapRef.get.map(_.get(key)).flatMap {
          case Some(actor) =>
            actor.submit(task)
          case None =>
            semaphore.withPermit {
              actorMapRef.get.map(_.get(key)).flatMap {
                case Some(actor) =>
                  actor.submit(task)
                case None =>
                  for {
                    actor <- FiberPool.Actor[F]
                    _ <- actorMapRef.update(
                      _.updated(key, actor)
                    )
                    join <- actor.submit(task)
                  } yield join
              }
            }
        }
    }
  }

  /** Cancel all existing background processors. */
  private def shutdown: F[Unit] = {
    semaphore.withPermit {
      for {
        _        <- isShutdownRef.set(true)
        actorMap <- actorMapRef.get
        _        <- actorMap.values.toList.traverse(_.shutdown)
      } yield ()
    }
  }
}

object FiberPool {

  private class Task[F[_]: Sync, A](
      deferred: Deferred[F, Either[Throwable, A]],
      task: F[A]
  ) {

    /** Execute the task and set the success/failure result on the deferred. */
    def execute: F[Unit] =
      task.attempt.flatMap(deferred.complete)

    /** Get the result of the execution, raising an error if it failed. */
    def join: F[A] =
      deferred.get.rethrow

    /** Signal to the submitter that the pool has been shut down. */
    def shutdown: F[Unit] =
      deferred
        .complete(Left(new RuntimeException("The pool has been shut down.")))
        .attempt
        .void
  }

  private class Actor[F[_]: Concurrent](
      queue: ConcurrentQueue[F, Task[F, _]],
      fiber: Fiber[F, Unit]
  ) {

    /** Submit a task to the queue, to be processed by the fiber. */
    def submit[A](task: F[A]): F[F[A]] =
      for {
        deferred <- Deferred[F, Either[Throwable, A]]
        wrapper = new Task(deferred, task)
        _ <- queue.offer(wrapper)
      } yield wrapper.join

    /** Cancel the processing and signal to all enqueued tasks that they will not be executed. */
    def shutdown: F[Unit] =
      for {
        _     <- fiber.cancel
        tasks <- queue.drain(0, Int.MaxValue)
        _     <- tasks.toList.traverse(_.shutdown)
      } yield ()
  }
  private object Actor {

    /** Execute all tasks in the queue. */
    def process[F[_]: Sync](queue: ConcurrentQueue[F, Task[F, _]]): F[Unit] =
      queue.poll.flatMap(_.execute) >> process(queue)

    /** Create an actor and start executing tasks in the background. */
    def apply[F[_]: Concurrent: ContextShift]: F[Actor[F]] =
      for {
        queue <- ConcurrentQueue.unbounded[F, Task[F, _]](None)
        fiber <- Concurrent[F].start(process(queue))
      } yield new Actor[F](queue, fiber)
  }

  /** Create an empty fiber pool. Cancel all fibers when it's released. */
  def apply[F[_]: Concurrent: ContextShift, PKey]
      : Resource[F, FiberPool[F, PKey]] =
    Resource.make(build[F, PKey])(_.shutdown)

  private def build[F[_]: Concurrent: ContextShift, PKey]
      : F[FiberPool[F, PKey]] =
    for {
      isShutdownRef <- Ref[F].of(false)
      actorMapRef   <- Ref[F].of(Map.empty[PKey, Actor[F]])
      semaphore     <- Semaphore[F](1)
      pool = new FiberPool[F, PKey](isShutdownRef, actorMapRef, semaphore)
    } yield pool
}
