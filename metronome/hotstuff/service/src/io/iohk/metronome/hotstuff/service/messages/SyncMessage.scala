package io.iohk.metronome.hotstuff.service.messages

import io.iohk.metronome.core.messages.{RPCMessage, RPCMessageCompanion}
import io.iohk.metronome.hotstuff.consensus.basic.Agreement

/** Messages facilitating synchronisation between nodes. */
sealed trait SyncMessage[A <: Agreement] extends RPCMessage

object SyncMessage extends RPCMessageCompanion {
  case class GetStatusRequest(
      requestId: RequestId
  ) extends SyncMessage[Nothing]
      with Request

  case class GetStatusResponse[A <: Agreement](
      requestId: RequestId,
      status: Nothing
  ) extends SyncMessage[A]
      with Response

  case class GetBlockRequest[A <: Agreement](
      requestId: RequestId,
      blockHash: A#Hash
  ) extends SyncMessage[Nothing]
      with Request

  case class GetBlockResponse[A <: Agreement](
      requestId: RequestId,
      block: A#Block
  ) extends SyncMessage[A]
      with Response
}
