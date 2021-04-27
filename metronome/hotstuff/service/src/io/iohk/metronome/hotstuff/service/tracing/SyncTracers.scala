package io.iohk.metronome.hotstuff.service.tracing

import cats.implicits._
import io.iohk.metronome.core.Validated
import io.iohk.metronome.tracer.Tracer
import io.iohk.metronome.hotstuff.consensus.basic.{Agreement, ProtocolError}
import io.iohk.metronome.hotstuff.service.messages.SyncMessage
import io.iohk.metronome.hotstuff.service.Status

case class SyncTracers[F[_], A <: Agreement](
    queueFull: Tracer[F, A#PKey],
    requestTimeout: Tracer[F, SyncTracers.Request[A]],
    responseIgnored: Tracer[F, SyncTracers.Response[A]],
    statusPoll: Tracer[F, SyncTracers.Statuses[A]],
    invalidStatus: Tracer[F, SyncTracers.StatusError[A]],
    error: Tracer[F, Throwable]
)

object SyncTracers {
  import SyncEvent._

  type Request[A <: Agreement] =
    (A#PKey, SyncMessage[A] with SyncMessage.Request)

  type Response[A <: Agreement] =
    (A#PKey, SyncMessage[A] with SyncMessage.Response)

  type Statuses[A <: Agreement] =
    Map[A#PKey, Validated[Status[A]]]

  type StatusError[A <: Agreement] =
    (Status[A], ProtocolError.InvalidQuorumCertificate[A], String)

  def apply[F[_], A <: Agreement](
      tracer: Tracer[F, SyncEvent[A]]
  ): SyncTracers[F, A] =
    SyncTracers[F, A](
      queueFull = tracer.contramap[A#PKey](QueueFull(_)),
      requestTimeout = tracer
        .contramap[Request[A]] { case (recipient, request) =>
          RequestTimeout(recipient, request)
        },
      responseIgnored = tracer
        .contramap[Response[A]] { case (sender, response) =>
          ResponseIgnored(sender, response)
        },
      statusPoll = tracer
        .contramap[Statuses[A]](StatusPoll(_)),
      invalidStatus =
        tracer.contramap[StatusError[A]] { case (status, error, hint) =>
          InvalidStatus(status, error, hint)
        },
      error = tracer.contramap[Throwable](Error(_))
    )
}
