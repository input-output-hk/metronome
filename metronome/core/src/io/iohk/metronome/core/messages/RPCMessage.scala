package io.iohk.metronome.core.messages

import java.util.UUID

/** Messages that go in request/response pairs. */
trait RPCMessage {

  /** Unique identifier for request, which is expected to be
    * included in the response message that comes back.
    */
  def requestId: UUID
}

abstract class RPCMessageCompanion {
  type RequestId = UUID
  object RequestId {
    def apply(): RequestId =
      UUID.randomUUID()
  }

  trait Request  extends RPCMessage
  trait Response extends RPCMessage
}
