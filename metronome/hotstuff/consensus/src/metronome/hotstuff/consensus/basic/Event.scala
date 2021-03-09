package metronome.hotstuff.consensus.basic

import metronome.hotstuff.consensus.ViewNumber

/** Input events for the protocol model. */
sealed trait Event[A <: Agreement]

object Event {

  /** A message received from a federation member. */
  case class MessageReceived[A <: Agreement](
      sender: A#PKey,
      message: Message[A]
  ) extends Event[A]

  /** A scheduled timeout for the round, initiating the next view. */
  case class NextView(viewNumber: ViewNumber) extends Event[Nothing]
}
