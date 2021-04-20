package io.iohk.metronome.hotstuff.service.tracing

import cats.implicits._
import io.iohk.metronome.tracer.Tracer
import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.hotstuff.service.messages.SyncMessage

case class SyncTracers[F[_], A <: Agreement](
    queueFull: Tracer[F, A#PKey],
    responseIgnored: Tracer[F, SyncMessage[A] with SyncMessage.Response],
    error: Tracer[F, Throwable]
)

object SyncTracers {
  import SyncEvent._

  def apply[F[_], A <: Agreement](
      tracer: Tracer[F, SyncEvent[A]]
  ): SyncTracers[F, A] =
    SyncTracers[F, A](
      queueFull = tracer.contramap[A#PKey](QueueFull(_)),
      responseIgnored = tracer
        .contramap[SyncMessage[A] with SyncMessage.Response](
          ResponseIgnored(_)
        ),
      error = tracer.contramap[Throwable](Error(_))
    )
}
