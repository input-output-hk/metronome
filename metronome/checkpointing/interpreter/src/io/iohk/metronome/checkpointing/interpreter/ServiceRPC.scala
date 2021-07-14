package io.iohk.metronome.checkpointing.interpreter

import io.iohk.metronome.checkpointing.models.Transaction

/** `ServiceRPC` is the interface that the Interpreter can call on the Service
  * side to send notifications. It provides RPC style methods for some of the
  * `InterpeterMessage` types, namely the ones `with Request with FromInterpeter`.
  *
  * See the `InterpreterMessage` for longer descriptions of the message types behind
  * the RPC facade.
  */
trait ServiceRPC[F[_]] {

  def newProposerBlock(
      proposerBlock: Transaction.ProposerBlock
  ): F[Unit]

  def newCheckpointCandidate: F[Unit]
}
