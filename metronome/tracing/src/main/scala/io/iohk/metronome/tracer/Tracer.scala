
import cats.{Applicative, Contravariant, FlatMap, Id, Monad, Monoid, Show, ~>}

/**
 * Contravariant tracer
 *
 * Ported from https://github.com/input-output-hk/contra-tracer/blob/master/src/Control/Tracer.hs
 */
trait Tracer[F[_], -A] extends Function[A, F[Unit]]

object Tracer {

  /**
   * If you know how to trace A and
   * - how to enrich A to get value B
   * - how to forget some details about B to get A
   * then you can create Tracer for B
   */
  implicit def contraTracer[F[_]]: Contravariant[Tracer[F, *]] =
    new Contravariant[Tracer[F, *]] {
      override def contramap[A, B](fa: Tracer[F, A])(f: B => A): Tracer[F, B] =
        a => fa(f(a))
    }

  def noOpTracer[M[_], A](implicit MA: Applicative[M]): Tracer[M, A] =
    _ => MA.pure(())

  implicit def monoidTracer[F[_], S](implicit MA: Applicative[F]): Monoid[Tracer[F, S]] =
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
  def contramapM[F[_], A, B](f: A => F[B], tracer: Tracer[F, B])(implicit MM: FlatMap[F]): Tracer[F, A] =
    (a: A) => MM.flatMap(f(a))(tracer)

  /** change the effect F to G using natural transformation nat */
  def natTracer[F[_], G[_], A](nat: F ~> G, tracer: Tracer[F, A]): Tracer[G, A] =
    a => nat(tracer(a))

  /** filter out values to trace if they do not pass predicate p  */
  def condTracing[F[_], A](p: A => Boolean, tr: Tracer[F, A])(implicit FM: Applicative[F]): Tracer[F, A] =
    (a: A) =>
      if (p(a)) tr(a)
      else FM.pure(())

  /** filter out values that was send to trace using side effecting predicate */
  def condTracingM[F[_], A](p: F[A => Boolean], tr: Tracer[F, A])(implicit FM: Monad[F]): Tracer[F, A] =
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
    event => f(event).foreach(t)
}

object TracerSyntax {

  implicit class TracerOps[F[_], A](val tracer: Tracer[F, A]) extends AnyVal {

    /** Trace value a using tracer */
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
