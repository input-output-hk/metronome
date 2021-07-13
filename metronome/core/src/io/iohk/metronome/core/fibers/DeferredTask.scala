package io.iohk.metronome.core.fibers

import cats.implicits._
import cats.effect.Sync
import cats.effect.concurrent.Deferred
import cats.effect.Concurrent
import scala.util.control.NoStackTrace

/** A task that can be executed on a fiber pool, or canceled if the pool is shut down.. */
protected[fibers] class DeferredTask[F[_]: Sync, A](
    deferred: Deferred[F, Either[Throwable, A]],
    task: F[A]
) {
  import DeferredTask.CanceledException

  /** Execute the task and set the success/failure result on the deferred. */
  def execute: F[Unit] =
    task.attempt.flatMap(deferred.complete)

  /** Get the result of the execution, raising an error if it failed. */
  def join: F[A] =
    deferred.get.rethrow

  /** Signal to the submitter that this task is canceled. */
  def cancel: F[Unit] =
    deferred
      .complete(Left(new CanceledException))
      .attempt
      .void
}

object DeferredTask {
  class CanceledException
      extends RuntimeException("This task has been canceled.")
      with NoStackTrace

  def apply[F[_]: Concurrent, A](task: F[A]): F[DeferredTask[F, A]] =
    Deferred[F, Either[Throwable, A]].map { d =>
      new DeferredTask[F, A](d, task)
    }
}
