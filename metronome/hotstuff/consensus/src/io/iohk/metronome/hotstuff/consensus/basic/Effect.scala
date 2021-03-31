package io.iohk.metronome.hotstuff.consensus.basic

import scala.concurrent.duration.FiniteDuration

import io.iohk.metronome.hotstuff.consensus.ViewNumber

/** Represent all possible effects that a protocol transition can
  * ask the host system to carry out, e.g. send messages to replicas.
  */
sealed trait Effect[+A <: Agreement]

object Effect {

  /** Schedule a callback after a timeout to initiate the next view
    * if the current rounds ends without an agreement.
    */
  case class ScheduleNextView(
      viewNumber: ViewNumber,
      timeout: FiniteDuration
  ) extends Effect[Nothing]

  /** Send a message to a federation member.
    *
    * The recipient can be the current member itself (i.e. the leader
    * sending itself a message to trigger its own vote). It is best
    * if the host system carries out these effects before it talks
    * to the external world, to avoid any possible phase mismatches.
    *
    * The `ProtocolState` could do it on its own but this way it's
    * slightly closer to the pseudo code.
    */
  case class SendMessage[A <: Agreement](
      recipient: A#PKey,
      message: Message[A]
  ) extends Effect[A]

  /** The leader of the round wants to propose a new block
    * on top of the last prepared one. The host environment
    * should consult the mempool and create one, passing the
    * result as an event.
    *
    * The block must be built as a child of `highQC.blockHash`.
    */
  case class CreateBlock[A <: Agreement](
      viewNumber: ViewNumber,
      highQC: QuorumCertificate[A]
  ) extends Effect[A]

  /** Execute blocks after a decision, from the last executed hash
    * up to the block included in the Quorum Certificate.
    */
  case class ExecuteBlocks[A <: Agreement](
      lastExecutedBlockHash: A#Hash,
      quorumCertificate: QuorumCertificate[A]
  ) extends Effect[A]

}
