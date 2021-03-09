package metronome.hotstuff.consensus.basic

import scala.concurrent.duration.FiniteDuration

import metronome.hotstuff.consensus.ViewNumber

/** Represent all possible effects that a protocol transition can
  * ask the host system to carry out, e.g. send messages to replicas.
  */
sealed trait Effect[A <: Agreement]

object Effect {

  /** Schedule a callback after a timeout to initiate the next view
    * if the current rounds ends without an agreement.
    */
  case class ScheduleNextView(
      viewNumber: ViewNumber,
      timeout: FiniteDuration
  ) extends Effect[Nothing]

  /** Send a message to a federation member. */
  case class SendMessage[A <: Agreement](
      recipient: A#PKey,
      message: Message[A]
  ) extends Effect[A]

  /** The leader of the round wants to propose a new block
    * on top of the last prepared one. The host environment
    * should consult the mempool and create one, passing the
    * result as an event.
    */
  case class CreateBlock[A <: Agreement](
      viewNumber: ViewNumber,
      parentBlockHash: A#Hash
  ) extends Effect[A]

  /** Execute blocks after a decision, up to the last executed hash. */
  case class ExecuteBlocks[A <: Agreement](
      lastExecutedBlockHash: A#Hash,
      decidedBlockHash: A#Hash
  ) extends Effect[A]

}
