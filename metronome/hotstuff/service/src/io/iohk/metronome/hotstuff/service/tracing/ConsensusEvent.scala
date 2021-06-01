package io.iohk.metronome.hotstuff.service.tracing

import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  Event,
  ProtocolError
}
import io.iohk.metronome.hotstuff.consensus.basic.{
  QuorumCertificate,
  VotingPhase
}
import io.iohk.metronome.hotstuff.service.ConsensusService.MessageCounter
import io.iohk.metronome.hotstuff.service.Status

sealed trait ConsensusEvent[+A <: Agreement]

object ConsensusEvent {

  /** The round ended without having reached decision. */
  case class Timeout(
      viewNumber: ViewNumber,
      messageCounter: MessageCounter
  ) extends ConsensusEvent[Nothing]

  /** A full view synchronization was requested after timing out without any in-sync messages. */
  case class ViewSync(
      viewNumber: ViewNumber
  ) extends ConsensusEvent[Nothing]

  /** Adopting the view of the federation after a sync. */
  case class AdoptView[A <: Agreement](
      status: Status[A]
  ) extends ConsensusEvent[A]

  /** The state advanced to a new view. */
  case class NewView(viewNumber: ViewNumber) extends ConsensusEvent[Nothing]

  /** Quorum over some block. */
  case class Quorum[A <: Agreement](
      quorumCertificate: QuorumCertificate[A, VotingPhase]
  ) extends ConsensusEvent[A]

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

  /** A block has been removed from storage by the time it was to be executed. */
  case class ExecutionSkipped[A <: Agreement](
      blockHash: A#Hash
  ) extends ConsensusEvent[A]

  /** A block has been executed. */
  case class BlockExecuted[A <: Agreement](
      blockHash: A#Hash
  ) extends ConsensusEvent[A]

  /** An unexpected error in one of the background tasks. */
  case class Error(
      message: String,
      error: Throwable
  ) extends ConsensusEvent[Nothing]
}
