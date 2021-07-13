package io.iohk.metronome.hotstuff.consensus.basic

import io.iohk.metronome.hotstuff.consensus.ViewNumber

/** Input events for the protocol model. */
sealed trait Event[+A <: Agreement]

object Event {

  /** A scheduled timeout for the round, initiating the next view. */
  case class NextView(viewNumber: ViewNumber) extends Event[Nothing]

  /** A message received from a federation member. */
  case class MessageReceived[A <: Agreement](
      sender: A#PKey,
      message: Message[A]
  ) extends Event[A]

  /** The block the leader asked to be created is ready. */
  case class BlockCreated[A <: Agreement](
      viewNumber: ViewNumber,
      block: A#Block,
      // The certificate which the block extended.
      highQC: QuorumCertificate[A]
  ) extends Event[A]
}
