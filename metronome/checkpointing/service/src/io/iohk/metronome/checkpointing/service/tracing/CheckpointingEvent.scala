package io.iohk.metronome.checkpointing.service.tracing

import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.checkpointing.interpreter.messages.InterpreterMessage
import io.iohk.metronome.checkpointing.service.messages.CheckpointingMessage

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

  /** An unexpected error. */
  case class Error(error: Throwable) extends CheckpointingEvent
}
