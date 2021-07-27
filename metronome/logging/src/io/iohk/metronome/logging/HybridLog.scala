package io.iohk.metronome.logging

import cats.implicits._
import cats.effect.{Clock, Sync}
import io.circe.JsonObject
import java.time.Instant
import scala.reflect.ClassTag
import java.util.concurrent.TimeUnit

/** Type class to transform instances of `T` to `HybridLogObject`.
  *
  * Using an effect because we attach a timestamp.
  */
trait HybridLog[F[_], T] {
  def apply(value: T): F[HybridLogObject]
}

object HybridLog {

  /** Create an instance of `HybridLog` for a type `T` by passing
    * functions to transform instances of `T` to message and JSON.
    */
  def instance[F[_]: Sync: Clock, T: ClassTag](
      level: T => HybridLogObject.Level,
      message: T => String,
      event: T => JsonObject
  ): HybridLog[F, T] =
    new HybridLog[F, T] {
      val source = implicitly[ClassTag[T]].runtimeClass.getName

      override def apply(value: T): F[HybridLogObject] =
        Clock[F].realTime(TimeUnit.MILLISECONDS).map { millis =>
          HybridLogObject(
            level = level(value),
            timestamp = Instant.ofEpochMilli(millis),
            source = source,
            message = message(value),
            event = event(value)
          )
        }
    }
}
