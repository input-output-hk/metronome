package io.iohk.metronome.core.fibers

import cats.implicits._
import cats.effect.{Concurrent, Fiber, Resource}
import cats.effect.concurrent.{Ref, Deferred}

/** Execute tasks in the background, canceling all fibers if the resource is released.
  *
  * Facilitates structured concurrency where the release of the component that submitted
  * these fibers causes the cancelation of all of its scheduled tasks.
  */
class FiberSet[F[_]: Concurrent](
    isShutdownRef: Ref[F, Boolean],
    fibersRef: Ref[F, Set[Fiber[F, Unit]]],
    tasksRef: Ref[F, Set[DeferredTask[F, _]]]
) {
  private def raiseIfShutdown: F[Unit] =
    isShutdownRef.get.ifM(
      Concurrent[F].raiseError(
        new IllegalStateException("The pool is already shut down.")
      ),
      ().pure[F]
    )

  def submit[A](task: F[A]): F[F[A]] = for {
    _             <- raiseIfShutdown
    deferredFiber <- Deferred[F, Fiber[F, Unit]]

    // Run the task, then remove the fiber from the tracker.
    background: F[A] = for {
      exec   <- task.attempt
      fiber  <- deferredFiber.get
      _      <- fibersRef.update(_ - fiber)
      result <- Concurrent[F].delay(exec).rethrow
    } yield result

    wrapper <- DeferredTask[F, A](background)
    _       <- tasksRef.update(_ + wrapper)

    // Start running in the background. Only now do we know the identity of the fiber.
    fiber <- Concurrent[F].start(wrapper.execute)

    // Add the fiber to the collectin first, so that if the effect is
    // already finished, it gets to remove it and we're not leaking memory.
    _ <- fibersRef.update(_ + fiber)
    _ <- deferredFiber.complete(fiber)

  } yield wrapper.join

  def shutdown: F[Unit] = for {
    _      <- isShutdownRef.set(true)
    fibers <- fibersRef.get
    _      <- fibers.toList.traverse(_.cancel)
    tasks  <- tasksRef.get
    _      <- tasks.toList.traverse(_.cancel)
  } yield ()
}

object FiberSet {
  def apply[F[_]: Concurrent]: Resource[F, FiberSet[F]] =
    Resource.make[F, FiberSet[F]] {
      for {
        isShutdownRef <- Ref[F].of(false)
        fibersRef     <- Ref[F].of(Set.empty[Fiber[F, Unit]])
        tasksRef      <- Ref[F].of(Set.empty[DeferredTask[F, _]])
      } yield new FiberSet[F](isShutdownRef, fibersRef, tasksRef)
    }(_.shutdown)
}
