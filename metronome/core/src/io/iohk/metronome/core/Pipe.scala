package io.iohk.metronome.core

import cats.implicits._
import cats.effect.{Concurrent, ContextShift, Sync}
import monix.tail.Iterant
import monix.catnap.ConcurrentQueue

/** A `Pipe` is a connection between two components where
  * messages of type `L` are going from left to right and
  * messages of type `R` are going from right to left.
  */
trait Pipe[F[_], L, R] {
  type Left  = Pipe.Side[F, L, R]
  type Right = Pipe.Side[F, R, L]

  def left: Left
  def right: Right
}
object Pipe {

  /** One side of a `Pipe` with
    * messages of type `I` going in and
    * messages of type `O` coming out.
    */
  trait Side[F[_], I, O] {
    def send(in: I): F[Unit]
    def receive: Iterant[F, O]
  }
  object Side {
    def apply[F[_]: Sync, I, O](
        iq: ConcurrentQueue[F, I],
        oq: ConcurrentQueue[F, O]
    ): Side[F, I, O] = new Side[F, I, O] {
      override def send(in: I): F[Unit] =
        iq.offer(in)

      override def receive: Iterant[F, O] =
        Iterant.repeatEvalF(oq.poll)
    }
  }

  def apply[F[_]: Concurrent: ContextShift, L, R]: F[Pipe[F, L, R]] =
    for {
      lq <- ConcurrentQueue.unbounded[F, L](None)
      rq <- ConcurrentQueue.unbounded[F, R](None)
    } yield new Pipe[F, L, R] {
      override val left  = Side[F, L, R](lq, rq)
      override val right = Side[F, R, L](rq, lq)
    }
}
