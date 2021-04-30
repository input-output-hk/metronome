package io.iohk.metronome.config

import io.circe._
import com.typesafe.config.{ConfigFactory, Config}
import scala.util.Try
import scala.concurrent.duration._

object ConfigDecoders {

  /** Parse a string into a TypeSafe config an use one of the accessor methods. */
  private def tryParse[T](value: String, f: (Config, String) => T): Try[T] =
    Try {
      val key  = "dummy"
      val conf = ConfigFactory.parseString(s"$key = $value")
      f(conf, key)
    }

  /** Parse HOCON byte sizes like "128M". */
  val bytesDecoder: Decoder[Long] =
    Decoder[String].emapTry {
      tryParse(_, _ getBytes _)
    }

  /** Parse HOCON durations like "5m". */
  val durationDecoder: Decoder[FiniteDuration] =
    Decoder[String].emapTry {
      tryParse(_, _.getDuration(_).toMillis.millis)
    }
}
