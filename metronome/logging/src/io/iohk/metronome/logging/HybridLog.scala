package io.iohk.metronome.logging

import io.circe.JsonObject
import java.time.Instant
import scala.reflect.ClassTag

/** Type class to transform instances of `T` to `HybridLogObject`. */
trait HybridLog[T] {
  def apply(value: T): HybridLogObject
}

object HybridLog {
  def apply[T](implicit ev: HybridLog[T]): HybridLog[T] = ev

  /** Create an instance of `HybridLog` for a type `T` by passing
    * functions to transform instances of `T` to message and JSON.
    */
  def instance[T: ClassTag](
      level: T => HybridLogObject.Level,
      message: T => String,
      event: T => JsonObject
  ): HybridLog[T] =
    new HybridLog[T] {
      val source = implicitly[ClassTag[T]].runtimeClass.getName

      override def apply(value: T): HybridLogObject = {
        HybridLogObject(
          level = level(value),
          timestamp = Instant.now(),
          source = source,
          message = message(value),
          event = event(value)
        )
      }
    }
}
