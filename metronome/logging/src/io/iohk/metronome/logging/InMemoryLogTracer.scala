package io.iohk.metronome.logging

import cats.implicits._
import cats.effect.Sync
import cats.effect.concurrent.Ref
import io.iohk.metronome.tracer.Tracer

/** Collect logs in memory, so we can inspect them in tests. */
object InMemoryLogTracer {

  class HybridLogTracer[F[_]: Sync](
      logRef: Ref[F, Vector[HybridLogObject]]
  ) extends Tracer[F, HybridLogObject] {

    override def apply(a: => HybridLogObject): F[Unit] =
      logRef.update(_ :+ a)

    def getLogs: F[Seq[HybridLogObject]] =
      logRef.get.map(_.toSeq)

    def getLevel(l: HybridLogObject.Level) =
      getLogs.map(_.filter(_.level == l))

    def getErrors = getLevel(HybridLogObject.Level.Error)
    def getWarns  = getLevel(HybridLogObject.Level.Warn)
    def getInfos  = getLevel(HybridLogObject.Level.Info)
    def getDebugs = getLevel(HybridLogObject.Level.Debug)
    def getTraces = getLevel(HybridLogObject.Level.Trace)

    def clear: F[Unit] = logRef.set(Vector.empty)
  }

  /** For example:
    *
    * ```
    * val logTracer = InMemoryLogTracer.hybrid[Task]
    * val networkEventTracer   = InMemoryLogTracer.hybrid[Task, NetworkEvent](logTracer)
    * val consensusEventTracer = InMemoryLogTracer.hybrid[Task, ConsensusEvent](logTracer)
    *
    * val test = for {
    *   msg   <- network.nextMessage
    *   _     <- consensus.handleMessage(msg)
    *   warns <- logTracer.getWarns
    * } yield {
    *   warns shouldBe empty
    * }
    *
    * ```
    */
  def hybrid[F[_]: Sync]: HybridLogTracer[F] =
    new HybridLogTracer[F](Ref.unsafe[F, Vector[HybridLogObject]](Vector.empty))

  def hybrid[F[_]: Sync, T](
      tracer: HybridLogTracer[F]
  )(implicit ev: HybridLog[F, T]): Tracer[F, T] =
    Tracer.contramapM[F, T, HybridLogObject](ev.apply _, tracer)
}
