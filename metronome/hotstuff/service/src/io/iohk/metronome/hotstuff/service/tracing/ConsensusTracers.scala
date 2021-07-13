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
import io.iohk.metronome.hotstuff.service.ConsensusService.MessageCounter
import io.iohk.metronome.hotstuff.service.Status

case class ConsensusTracers[F[_], A <: Agreement](
    timeout: Tracer[F, (ViewNumber, MessageCounter)],
    viewSync: Tracer[F, ViewNumber],
    adoptView: Tracer[F, Status[A]],
    newView: Tracer[F, ViewNumber],
    quorum: Tracer[F, QuorumCertificate[A]],
    fromPast: Tracer[F, Event.MessageReceived[A]],
    fromFuture: Tracer[F, Event.MessageReceived[A]],
    stashed: Tracer[F, ProtocolError.TooEarly[A]],
    rejected: Tracer[F, ProtocolError[A]],
    executionSkipped: Tracer[F, A#Hash],
    blockExecuted: Tracer[F, A#Hash],
    error: Tracer[F, (String, Throwable)]
)

object ConsensusTracers {
  import ConsensusEvent._

  def apply[F[_], A <: Agreement](
      tracer: Tracer[F, ConsensusEvent[A]]
  ): ConsensusTracers[F, A] =
    ConsensusTracers[F, A](
      timeout = tracer.contramap[(ViewNumber, MessageCounter)](
        (Timeout.apply _).tupled
      ),
      viewSync = tracer.contramap[ViewNumber](ViewSync(_)),
      adoptView = tracer.contramap[Status[A]](AdoptView(_)),
      newView = tracer.contramap[ViewNumber](NewView(_)),
      quorum = tracer.contramap[QuorumCertificate[A]](Quorum(_)),
      fromPast = tracer.contramap[Event.MessageReceived[A]](FromPast(_)),
      fromFuture = tracer.contramap[Event.MessageReceived[A]](FromFuture(_)),
      stashed = tracer.contramap[ProtocolError.TooEarly[A]](Stashed(_)),
      rejected = tracer.contramap[ProtocolError[A]](Rejected(_)),
      executionSkipped = tracer.contramap[A#Hash](ExecutionSkipped(_)),
      blockExecuted = tracer.contramap[A#Hash](BlockExecuted(_)),
      error = tracer.contramap[(String, Throwable)]((Error.apply _).tupled)
    )
}
