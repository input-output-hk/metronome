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
      highQC: QuorumCertificate[A, Phase.Prepare]
  ) extends Effect[A]

  /** Once the Prepare Q.C. has been established for a block,
    * we know that it's not spam, it's safe to be persisted.
    *
    * This prevents a rouge leader from sending us many `Prepare`
    * messages in the same view with the intention of eating up
    * space using the included block.
    *
    * It's also a way for us to delay saving a block we created
    * as a leader to the time when it's been voted on. Since it's
    * part of the `Prepare` message, replicas shouldn't be asking
    * for it anyway, so it's not a problem if it's not yet persisted.
    */
  case class SaveBlock[A <: Agreement](
      preparedBlock: A#Block
  ) extends Effect[A]

  /** Execute blocks after a decision, from the last executed hash
    * up to the block included in the Quorum Certificate.
    */
  case class ExecuteBlocks[A <: Agreement](
      lastExecutedBlockHash: A#Hash,
      quorumCertificate: QuorumCertificate[A, Phase.Commit]
  ) extends Effect[A]

}
