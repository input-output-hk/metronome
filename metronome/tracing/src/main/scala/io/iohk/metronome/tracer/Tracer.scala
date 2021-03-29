package io.iohk.metronome.tracer

import language.higherKinds
import cats.{Applicative, Contravariant, FlatMap, Id, Monad, Monoid, Show, ~>}

/** Contravariant tracer.
  *
  * Ported from https://github.com/input-output-hk/contra-tracer/blob/master/src/Control/Tracer.hs
  */
trait Tracer[F[_], -A] {
  def apply(a: => A): F[Unit]
}

object Tracer {

  def instance[F[_], A](f: (=> A) => F[Unit]): Tracer[F, A] =
    new Tracer[F, A] {
      override def apply(a: => A): F[Unit] = f(a)
    }

  def const[F[_], A](f: F[Unit]): Tracer[F, A] =
    instance(_ => f)

  /** If you know:
    * - how to enrich type A that is traced
    * - how to squeeze B's to create A's (possibly enrich B with extra stuff, or forget some details)
    * then you have Tracer for B
    *
    * To use this, just import `cats` syntax for `Contravariant` and call `.contramap` on `A`.
    */
  implicit def contraTracer[F[_]]: Contravariant[Tracer[F, *]] =
    new Contravariant[Tracer[F, *]] {
      override def contramap[A, B](fa: Tracer[F, A])(f: B => A): Tracer[F, B] =
        new Tracer[F, B] {
          override def apply(a: => B): F[Unit] = fa(f(a))
        }
    }

  def noOpTracer[M[_], A](implicit MA: Applicative[M]): Tracer[M, A] =
    new Tracer[M, A] {
      override def apply(a: => A): M[Unit] = MA.pure(())
    }

  implicit def monoidTracer[F[_], S](implicit
      MA: Applicative[F]
  ): Monoid[Tracer[F, S]] =
    new Monoid[Tracer[F, S]] {

      /** Run sequentially two tracers */
      override def combine(a1: Tracer[F, S], a2: Tracer[F, S]): Tracer[F, S] =
        s => MA.productR(a1(s))(a2(s))

      override def empty: Tracer[F, S] = noOpTracer
    }

  /** Trace value a using tracer tracer */
  def traceWith[F[_], A](tracer: Tracer[F, A], a: A): F[Unit] = tracer(a)

  /** contravariant Kleisli composition:
    * if you can:
    * - produce effect M[B] from A
    * - trace B's
    * then you can trace A's
    */
  def contramapM[F[_], A, B](f: A => F[B], tracer: Tracer[F, B])(implicit
      MM: FlatMap[F]
  ): Tracer[F, A] = {
    new Tracer[F, A] {
      override def apply(a: => A): F[Unit] =
        MM.flatMap(f(a))(tracer(_))
    }
  }

  /** change the effect F to G using natural transformation nat */
  def natTracer[F[_], G[_], A](
      nat: F ~> G,
      tracer: Tracer[F, A]
  ): Tracer[G, A] =
    a => nat(tracer(a))

  /** filter out values to trace if they do not pass predicate p */
  def condTracing[F[_], A](p: A => Boolean, tr: Tracer[F, A])(implicit
      FM: Applicative[F]
  ): Tracer[F, A] = {
    new Tracer[F, A] {
      override def apply(a: => A): F[Unit] =
        if (p(a)) tr(a)
        else FM.pure(())
    }
  }

  /** filter out values that was send to trace using side effecting predicate */
  def condTracingM[F[_], A](p: F[A => Boolean], tr: Tracer[F, A])(implicit
      FM: Monad[F]
  ): Tracer[F, A] =
    a =>
      FM.flatMap(p) { pa =>
        if (pa(a)) tr(a)
        else FM.pure(())
      }

  def showTracing[F[_], A](
      tracer: Tracer[F, String]
  )(implicit S: Show[A], C: Contravariant[Tracer[F, *]]): Tracer[F, A] =
    C.contramap(tracer)(S.show)

  def traceAll[A, B](f: B => List[A], t: Tracer[Id, A]): Tracer[Id, B] =
    new Tracer[Id, B] {
      override def apply(event: => B): Id[Unit] = f(event).foreach(t(_))
    }
}

object TracerSyntax {

  implicit class TracerOps[F[_], A](val tracer: Tracer[F, A]) extends AnyVal {

    /** Trace value a using tracer tracer */
    def trace(a: A): F[Unit] = tracer(a)

    /** contravariant Kleisli composition:
      * if you can:
      * - produce effect M[B] from A
      * - trace B's
      * then you can trace A's
      */
    def >=>[B](f: B => F[A])(implicit MM: FlatMap[F]): Tracer[F, B] =
      Tracer.contramapM(f, tracer)

    def nat[G[_]](nat: F ~> G): Tracer[G, A] =
      Tracer.natTracer(nat, tracer)

    def filter(p: A => Boolean)(implicit FM: Applicative[F]): Tracer[F, A] =
      Tracer.condTracing[F, A](p, tracer)

    def filterNot(p: A => Boolean)(implicit FM: Applicative[F]): Tracer[F, A] =
      filter(a => !p(a))

    def filterM(p: F[A => Boolean])(implicit FM: Monad[F]): Tracer[F, A] =
      Tracer.condTracingM(p, tracer)
  }
}
