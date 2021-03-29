package io.iohk.metronome.logging

import io.circe.JsonObject
import io.circe.syntax._
import java.time.Instant
import scala.reflect.ClassTag
import cats.Show

/** A hybrid log has a human readable message, which is intended to be static,
  * and some key-value paramters that vary by events.
  *
  * See https://medium.com/unomaly/logging-wisdom-how-to-log-5a19145e35ec
  */
case class HybridLogObject(
    timestamp: Instant,
    source: String,
    level: HybridLogObject.Level,
    // Something captured about what emitted this event.
    // Human readable message, which typically shouldn't
    // change between events emitted at the same place.
    message: String,
    // Key-Value pairs that capture arbitrary data.
    event: JsonObject
)
object HybridLogObject {
  sealed trait Level
  object Level {
    case object Error extends Level
    case object Warn  extends Level
    case object Info  extends Level
    case object Debug extends Level
    case object Trace extends Level
  }

  implicit val show: Show[HybridLogObject] = Show.show {
    case HybridLogObject(t, s, l, m, e) =>
      s"$t ${l.toString.toUpperCase.padTo(5, ' ')} - $s: $m ${e.asJson.noSpaces}"
  }
}
