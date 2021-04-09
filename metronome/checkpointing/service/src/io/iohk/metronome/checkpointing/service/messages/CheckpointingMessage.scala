package io.iohk.metronome.checkpointing.service.messages

import io.iohk.metronome.core.messages.{RPCMessage, RPCMessageCompanion}
import io.iohk.metronome.checkpointing.models.Ledger

/** Checkpointing specific messages. */
sealed trait CheckpointingMessage extends RPCMessage

object CheckpointingMessage extends RPCMessageCompanion {

  case class GetStateRequest(
      requestId: RequestId,
      stateHash: Ledger.Hash
  ) extends CheckpointingMessage
      with Request

  case class GetStateResponse(
      requestId: RequestId,
      state: Ledger
  ) extends CheckpointingMessage
      with Response
}
