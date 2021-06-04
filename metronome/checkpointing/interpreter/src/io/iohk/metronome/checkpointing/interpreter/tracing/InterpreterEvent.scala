package io.iohk.metronome.checkpointing.interpreter.tracing

import io.iohk.metronome.checkpointing.interpreter.messages.InterpreterMessage
import io.iohk.metronome.checkpointing.interpreter.messages.InterpreterMessage._

sealed trait InterpreterEvent

object InterpreterEvent {

  /** The PoW Interpreter did not produce a response in time. */
  case class InterpreterTimeout(
      message: InterpreterMessage with Request with FromService
  ) extends InterpreterEvent

  /** The Service could not be reached. */
  case class ServiceUnavailable(
      message: InterpreterMessage with FromInterpreter
  ) extends InterpreterEvent

  /** An unexpected error. */
  case class Error(error: Throwable) extends InterpreterEvent
}
