package io.iohk.metronome.logging

import io.circe.JsonObject
import java.time.Instant
import scala.reflect.ClassTag

trait HybridLog[T] {
  def apply(value: T): HybridLogObject
}

object HybridLog {
  def apply[T](implicit ev: HybridLog[T]): HybridLog[T] = ev

  def instance[T: ClassTag](
      level: T => HybridLogObject.Level,
      message: T => String,
      event: T => JsonObject,
      source: Option[String] = None
  ): HybridLog[T] =
    new HybridLog[T] {
      val className = implicitly[ClassTag[T]].runtimeClass.getName

      override def apply(value: T): HybridLogObject = {
        HybridLogObject(
          level = level(value),
          timestamp = Instant.now(),
          source = source getOrElse className,
          message = message(value),
          event = event(value)
        )
      }
    }
}
