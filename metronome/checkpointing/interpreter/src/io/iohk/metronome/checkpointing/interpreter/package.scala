package io.iohk.metronome.checkpointing

import io.iohk.metronome.networking.LocalConnectionManager
import io.iohk.metronome.checkpointing.interpreter.messages.InterpreterMessage

package object interpreter {
  type InterpreterConnection[F[_]] =
    LocalConnectionManager[
      F,
      CheckpointingAgreement.PKey,
      InterpreterMessage
    ]
}
