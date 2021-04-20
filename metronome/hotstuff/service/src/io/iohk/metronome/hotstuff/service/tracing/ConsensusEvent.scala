package io.iohk.metronome.hotstuff.service.tracing

import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  Event,
  ProtocolError
}
import io.iohk.metronome.hotstuff.consensus.basic.QuorumCertificate

sealed trait ConsensusEvent[+A <: Agreement]

object ConsensusEvent {

  /** The round ended without having reached decision. */
  case class Timeout(viewNumber: ViewNumber) extends ConsensusEvent[Nothing]

  /** The state advanced to a new view. */
  case class NewView(viewNumber: ViewNumber) extends ConsensusEvent[Nothing]

  /** Quorum over some block. */
  case class Quorum[A <: Agreement](quorumCertificate: QuorumCertificate[A])
      extends ConsensusEvent[A]

  /** A formally valid message was received from an earlier view number. */
  case class FromPast[A <: Agreement](message: Event.MessageReceived[A])
      extends ConsensusEvent[A]

  /** A formally valid message was received from a future view number. */
  case class FromFuture[A <: Agreement](message: Event.MessageReceived[A])
      extends ConsensusEvent[A]

  /** An event that arrived too early but got stashed and will be redelivered. */
  case class Stashed[A <: Agreement](
      error: ProtocolError.TooEarly[A]
  ) extends ConsensusEvent[A]

  /** A rejected event. */
  case class Rejected[A <: Agreement](
      error: ProtocolError[A]
  ) extends ConsensusEvent[A]

  /** An unexpected error in one of the background tasks. */
  case class Error(
      error: Throwable
  ) extends ConsensusEvent[Nothing]
}
