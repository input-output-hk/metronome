package io.iohk.metronome.hotstuff.service.messages

import io.iohk.metronome.core.messages.{RPCMessage, RPCMessageCompanion}
import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.hotstuff.service.Status

/** Messages needed to fully realise the HotStuff protocol,
  * without catering for any application specific concerns.
  */
sealed trait SyncMessage[+A <: Agreement] { self: RPCMessage => }

object SyncMessage extends RPCMessageCompanion {
  case class GetStatusRequest[A <: Agreement](
      requestId: RequestId
  ) extends SyncMessage[A]
      with Request

  case class GetStatusResponse[A <: Agreement](
      requestId: RequestId,
      status: Status[A]
  ) extends SyncMessage[A]
      with Response

  case class GetBlockRequest[A <: Agreement](
      requestId: RequestId,
      blockHash: A#Hash
  ) extends SyncMessage[A]
      with Request

  case class GetBlockResponse[A <: Agreement](
      requestId: RequestId,
      block: A#Block
  ) extends SyncMessage[A]
      with Response

  implicit def getBlockPair[A <: Agreement] =
    pair[GetBlockRequest[A], GetBlockResponse[A]]

  implicit def getStatusPair[A <: Agreement] =
    pair[GetStatusRequest[A], GetStatusResponse[A]]
}
