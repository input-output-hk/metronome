package io.iohk.metronome.logging

import cats.syntax.contravariant._
import cats.effect.Sync
import io.circe.syntax._
import io.iohk.metronome.tracer.Tracer
import org.slf4j.LoggerFactory

/** Forward traces to SLF4J logs. */
object LogTracer {

  /** Create a logger for `HybridLogObject` that delegates to SLF4J. */
  def hybrid[F[_]: Sync]: Tracer[F, HybridLogObject] =
    new Tracer[F, HybridLogObject] {
      override def apply(log: => HybridLogObject): F[Unit] = Sync[F].delay {
        val logger = LoggerFactory.getLogger(log.source)

        def message = s"${log.message} ${log.event.asJson.noSpaces}"

        log.level match {
          case HybridLogObject.Level.Error =>
            if (logger.isErrorEnabled) logger.error(message)
          case HybridLogObject.Level.Warn =>
            if (logger.isWarnEnabled) logger.warn(message)
          case HybridLogObject.Level.Info =>
            if (logger.isInfoEnabled) logger.info(message)
          case HybridLogObject.Level.Debug =>
            if (logger.isDebugEnabled) logger.debug(message)
          case HybridLogObject.Level.Trace =>
            if (logger.isTraceEnabled) logger.trace(message)
        }
      }
    }

  /** Create a logger for a type that can be transformed to a `HybridLogObject`. */
  def hybrid[F[_]: Sync, T: HybridLog]: Tracer[F, T] =
    hybrid[F].contramap(implicitly[HybridLog[T]].apply _)
}
