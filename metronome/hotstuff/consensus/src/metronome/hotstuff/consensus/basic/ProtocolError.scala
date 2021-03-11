package metronome.hotstuff.consensus.basic

import metronome.hotstuff.consensus.ViewNumber

sealed trait ProtocolError[A <: Agreement]

object ProtocolError {

  /** A leader message was received from a replica that isn't the leader of the view. */
  case class NotFromLeader[A <: Agreement](
      event: Event.MessageReceived[A],
      expected: A#PKey
  ) extends ProtocolError[A]

  /** A replica message was received in a view that this replica is not leading. */
  case class NotToLeader[A <: Agreement](
      event: Event.MessageReceived[A],
      expected: A#PKey
  ) extends ProtocolError[A]

  /** A message coming from outside the federation members. */
  case class NotFromFederation[A <: Agreement](
      event: Event.MessageReceived[A]
  ) extends ProtocolError[A]

  /** A message was received from a different view. */
  case class WrongViewNumber[A <: Agreement](
      event: Event.MessageReceived[A],
      expected: ViewNumber
  ) extends ProtocolError[A]

  /** A message was received from a different phase.
    * This is normal for the leader, who after n-f votes moves to the next phase.
    */
  case class WrongPhase[A <: Agreement](
      event: Event.MessageReceived[A],
      expected: Phase
  ) extends ProtocolError[A]

  /** The vote signature doesn't match the content. */
  case class InvalidVote[A <: Agreement](
      sender: A#PKey,
      message: Message.Vote[A]
  ) extends ProtocolError[A]

  /** The Q.C. signature doesn't match the content. */
  case class InvalidQuorumCertificate[A <: Agreement](
      sender: A#PKey,
      quorumCertificate: QuorumCertificate[A]
  ) extends ProtocolError[A]

  /** The block in the prepare message doesn't extend the previous Q.C. */
  case class UnsafeExtension[A <: Agreement](
      sender: A#PKey,
      message: Message.Prepare[A]
  ) extends ProtocolError[A]

  /** A message we didn't expect to receive in the given state. */
  case class Unexpected[A <: Agreement](
      event: Event.MessageReceived[A]
  ) extends ProtocolError[A]
}