package io.iohk.metronome.checkpointing.service.messages

import io.iohk.metronome.core.messages.{RPCMessage, RPCMessageCompanion}
import io.iohk.metronome.checkpointing.models.Ledger

/** Checkpointing specific messages that the HotStuff service doesn't handle,
  * which is the synchronisation of committed ledger state.
  *
  * These will be wrapped in a `RemoteMessage.ApplicationMessage`.
  */
sealed trait CheckpointingMessage extends RPCMessage

object CheckpointingMessage extends RPCMessageCompanion {

  /** Request the ledger state given by a specific hash.
    *
    * The hash is something coming from a block that was
    * pointed at by a Commit Q.C.
    */
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
