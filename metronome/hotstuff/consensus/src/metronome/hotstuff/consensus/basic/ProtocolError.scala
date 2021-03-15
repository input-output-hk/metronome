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
  case class UnexpectedBlockHash[A <: Agreement](
      event: Event.MessageReceived[A],
      expected: A#Hash
  ) extends ProtocolError[A]

  /** A message that we received slightly earlier than we expected.
    *
    * One reason for this could be that the peer is slightly ahead of us,
    * e.g. already finished the `Decide` phase and sent out the `NewView`
    * to us, the next leader, in which case the view number would not
    * match up. Or maybe a quorum has already formed for the next round
    * and we receive a `Prepare`, while we're still in `Decide`.
    *
    * The host system passing the events and processing the effects
    * is expected to inspect `TooEarly` messages and decide what to do:
    * - if the message is for the next round or next phase, then just re-deliver it after the view transition
    * - if the message is far in the future, perhaps it's best to re-sync the status with everyone
    */
  case class TooEarly[A <: Agreement](
      event: Event.MessageReceived[A],
      expectedInViewNumber: ViewNumber,
      expectedInPhase: Phase
  ) extends ProtocolError[A]

  /** A message we didn't expect to receive in the given state.
    *
    * The host system can maintain some metrics so we can see if we're completely out of
    * alignment with all the other peers.
    */
  case class Unexpected[A <: Agreement](
      event: Event.MessageReceived[A]
  ) extends ProtocolError[A]
}
