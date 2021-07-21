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
  implicit lazy val durationDecoder: Decoder[FiniteDuration] =
    Decoder[String].emapTry {
      tryParse(_, _.getDuration(_).toMillis.millis)
    }

  /** Overriding boolean values with system properties turns them into String,
    * which the default circe decoder does not expect.
    */
  implicit lazy val booleanDecoder: Decoder[Boolean] =
    Decoder.decodeBoolean or Decoder[String].emapTry {
      tryParse(_, _ getBoolean _)
    }

  /** Parse an object where a discriminant tells us which other key value
    * to deserialise into the target type.
    *
    * For example take the following config:
    *
    * ```
    * virus {
    *   variant = alpha
    *   alpha {
    *     r = 1.1
    *   }
    *   delta {
    *     r = 1.4
    *   }
    * }
    * ```
    *
    * It should deserialize into a class that matches a sub-key:
    * ```
    * case class Virus(r: Double)
    * object Virus {
    *   implicit val decoder: Decoder[Virus] =
    *     ConfigDecoders.strategyDecoder[Virus]("variant", deriveDecoder)
    * }
    * ```
    *
    * The decoder will deserialise all the other keys as well to make sure
    * that all of them are valid, in case the selection changes.
    */
  def strategyDecoder[T](
      discriminant: String,
      decoder: Decoder[T]
  ): Decoder[T] = {
    // This parser is applied after the fields have been transformed to camelCase.
    import ConfigParser.toCamelCase
    // Not passing the decoder implicitly so the compiler doesn't pass
    // the one we are constructing here.
    implicit val inner: Decoder[T] = decoder

    Decoder.instance[T] { (c: HCursor) =>
      for {
        obj <- c.value.as[JsonObject]
        ccd = toCamelCase(discriminant)
        selected <- c.downField(ccd).as[String].map(toCamelCase)
        value    <- c.downField(selected).as[T]
        // Making sure that everything else is valid. We could pick the value from the result,
        // but it's more difficult to provide the right `DecodingFailure` with a list of operations
        // if the selected key is not present in the map.
        _ <- Json.fromJsonObject(obj.remove(ccd)).as[Map[String, T]]
      } yield value
    }
  }
}
