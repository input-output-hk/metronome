package io.iohk.metronome.hotstuff.service.tracing

import cats.implicits._
import io.iohk.metronome.tracer.Tracer
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  Event,
  ProtocolError,
  QuorumCertificate
}

case class ConsensusTracers[F[_], A <: Agreement](
    timeout: Tracer[F, ViewNumber],
    newView: Tracer[F, ViewNumber],
    quorum: Tracer[F, QuorumCertificate[A]],
    fromPast: Tracer[F, Event.MessageReceived[A]],
    fromFuture: Tracer[F, Event.MessageReceived[A]],
    stashed: Tracer[F, ProtocolError.TooEarly[A]],
    rejected: Tracer[F, ProtocolError[A]],
    error: Tracer[F, Throwable]
)

object ConsensusTracers {
  import ConsensusEvent._

  def apply[F[_], A <: Agreement](
      tracer: Tracer[F, ConsensusEvent[A]]
  ): ConsensusTracers[F, A] =
    ConsensusTracers[F, A](
      timeout = tracer.contramap[ViewNumber](Timeout(_)),
      newView = tracer.contramap[ViewNumber](NewView(_)),
      quorum = tracer.contramap[QuorumCertificate[A]](Quorum(_)),
      fromPast = tracer.contramap[Event.MessageReceived[A]](FromPast(_)),
      fromFuture = tracer.contramap[Event.MessageReceived[A]](FromFuture(_)),
      stashed = tracer.contramap[ProtocolError.TooEarly[A]](Stashed(_)),
      rejected = tracer.contramap[ProtocolError[A]](Rejected(_)),
      error = tracer.contramap[Throwable](Error(_))
    )
}
