package io.iohk.metronome.core.fibers

import cats.implicits._
import cats.effect.{Sync, Concurrent, ContextShift, Fiber, Resource}
import cats.effect.concurrent.{Ref, Semaphore, Deferred}
import monix.catnap.ConcurrentQueue
import monix.execution.BufferCapacity
import monix.execution.ChannelType
import scala.util.control.NoStackTrace

/** Execute tasks on a separate fiber per source key,
  * facilitating separate rate limiting and fair concurrency.
  *
  * Each fiber executes tasks one by one.
  */
class FiberMap[F[_]: Concurrent: ContextShift, K](
    isShutdownRef: Ref[F, Boolean],
    actorMapRef: Ref[F, Map[K, FiberMap.Actor[F]]],
    semaphore: Semaphore[F],
    capacity: BufferCapacity
) {

  /** Submit a task to be processed in the background.
    *
    * Create a new fiber if the given key hasn't got one yet.
    *
    * The result can be waited upon or discarded, the processing
    * will happen in the background regardless.
    */
  def submit[A](key: K)(task: F[A]): F[F[A]] = {
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
                    actor <- FiberMap.Actor[F](capacity)
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

object FiberMap {

  /** The queue of a key is at capacity and didn't accept the task. */
  class QueueFullException
      extends RuntimeException("The fiber task queue is full.")
      with NoStackTrace

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

    private val reject = Sync[F].raiseError[Unit](new QueueFullException)

    /** Submit a task to the queue, to be processed by the fiber.
      *
      * If the queue is full, a `QueueFullException` is raised so the submitting
      * process knows that this key is producing too much data.
      */
    def submit[A](task: F[A]): F[F[A]] =
      for {
        deferred <- Deferred[F, Either[Throwable, A]]
        wrapper = new Task(deferred, task)
        enqueued <- queue.tryOffer(wrapper)
        _        <- reject.whenA(!enqueued)
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
    def apply[F[_]: Concurrent: ContextShift](
        capacity: BufferCapacity
    ): F[Actor[F]] =
      for {
        queue <- ConcurrentQueue
          .withConfig[F, Task[F, _]](capacity, ChannelType.MPSC)
        fiber <- Concurrent[F].start(process(queue))
      } yield new Actor[F](queue, fiber)
  }

  /** Create an empty fiber pool. Cancel all fibers when it's released. */
  def apply[F[_]: Concurrent: ContextShift, K](
      capacity: BufferCapacity = BufferCapacity.Unbounded(None)
  ): Resource[F, FiberMap[F, K]] =
    Resource.make(build[F, K](capacity))(_.shutdown)

  private def build[F[_]: Concurrent: ContextShift, K](
      capacity: BufferCapacity
  ): F[FiberMap[F, K]] =
    for {
      isShutdownRef <- Ref[F].of(false)
      actorMapRef   <- Ref[F].of(Map.empty[K, Actor[F]])
      semaphore     <- Semaphore[F](1)
      pool = new FiberMap[F, K](
        isShutdownRef,
        actorMapRef,
        semaphore,
        capacity
      )
    } yield pool
}
