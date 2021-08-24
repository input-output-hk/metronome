package io.iohk.metronome.checkpointing.service.tracing

import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.checkpointing.interpreter.messages.InterpreterMessage
import io.iohk.metronome.checkpointing.service.messages.CheckpointingMessage
import io.iohk.metronome.checkpointing.models.{Block, Ledger}
import io.iohk.metronome.checkpointing.models.CheckpointCertificate

sealed trait CheckpointingEvent

object CheckpointingEvent {
  import InterpreterMessage._

  /** The Interpreter did not produce a response in time. */
  case class InterpreterTimeout(
      message: InterpreterMessage with Request with FromService
  ) extends CheckpointingEvent

  /** The Interpreter could not be reached. */
  case class InterpreterUnavailable(
      message: InterpreterMessage with FromService
  ) extends CheckpointingEvent

  /** The Interpreter sent us a response which we ignored, most likely because it was late. */
  case class InterpreterResponseIgnored(
      message: InterpreterMessage with Response with FromInterpreter,
      maybeError: Option[Throwable]
  ) extends CheckpointingEvent

  /** A peer did not produce a response in time. */
  case class NetworkTimeout(
      recipient: CheckpointingAgreement.PKey,
      message: CheckpointingMessage with CheckpointingMessage.Request
  ) extends CheckpointingEvent

  /** A peer sent an unsolicited response, or the response arrived too late. */
  case class NetworkResponseIgnored(
      from: CheckpointingAgreement.PKey,
      message: CheckpointingMessage with CheckpointingMessage.Response,
      maybeError: Option[Throwable]
  ) extends CheckpointingEvent

  /** This node has created a block, to be proposed to the federation. */
  case class Proposing(
      block: Block
  ) extends CheckpointingEvent

  /** The federation committed to a new state. */
  case class NewState(state: Ledger) extends CheckpointingEvent

  /** Pushing a new certificate to the interpreter. */
  case class NewCheckpointCertificate(certificate: CheckpointCertificate)
      extends CheckpointingEvent

  /** A block could not be validated because we could not produce the corresponding state. */
  case class StateUnavailable(block: Block) extends CheckpointingEvent

  /** The interpreter thought the block was invalid. */
  case class InterpreterValidationFailed(block: Block)
      extends CheckpointingEvent

  /** An unexpected error. */
  case class Error(error: Throwable) extends CheckpointingEvent
}
