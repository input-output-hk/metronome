package io.iohk.metronome.hotstuff.service.tracing

import cats.implicits._
import io.iohk.metronome.tracer.Tracer
import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.hotstuff.service.messages.SyncMessage
import io.iohk.metronome.hotstuff.service.Status

case class SyncTracers[F[_], A <: Agreement](
    queueFull: Tracer[F, A#PKey],
    requestTimeout: Tracer[F, SyncTracers.Request[A]],
    responseIgnored: Tracer[F, SyncTracers.Response[A]],
    statusPoll: Tracer[F, SyncTracers.Statuses[A]],
    error: Tracer[F, Throwable]
)

object SyncTracers {
  import SyncEvent._

  type Request[A <: Agreement] =
    (A#PKey, SyncMessage[A] with SyncMessage.Request)

  type Response[A <: Agreement] =
    (A#PKey, SyncMessage[A] with SyncMessage.Response, Option[Throwable])

  type Statuses[A <: Agreement] =
    (IndexedSeq[A#PKey], IndexedSeq[Option[Status[A]]])

  def apply[F[_], A <: Agreement](
      tracer: Tracer[F, SyncEvent[A]]
  ): SyncTracers[F, A] =
    SyncTracers[F, A](
      queueFull = tracer.contramap[A#PKey](QueueFull(_)),
      requestTimeout = tracer
        .contramap[Request[A]]((RequestTimeout.apply[A] _).tupled),
      responseIgnored = tracer
        .contramap[Response[A]]((ResponseIgnored.apply[A] _).tupled),
      statusPoll = tracer
        .contramap[Statuses[A]] { case (publicKeys, maybeStatuses) =>
          StatusPoll[A]((publicKeys zip maybeStatuses).toMap)
        },
      error = tracer.contramap[Throwable](Error(_))
    )
}
