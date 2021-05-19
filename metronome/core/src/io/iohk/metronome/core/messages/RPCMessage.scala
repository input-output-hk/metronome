package io.iohk.metronome.core.messages

import cats.effect.Sync
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

    def apply[F[_]: Sync]: F[RequestId] =
      Sync[F].delay(apply())
  }

  trait Request  extends RPCMessage
  trait Response extends RPCMessage

  /** Establish a relationship between a request and a response
    * type so the compiler can infer the return value of methods
    * based on the request parameter, or validate that two generic
    * parameters belong with each other.
    */
  def pair[A <: Request, B <: Response]: RPCPair.Aux[A, B] =
    new RPCPair[A] { type Response = B }
}

/** A request can be associated with at most one response type.
  * On the other hand a response type can serve multiple requests.
  */
trait RPCPair[Request] {
  type Response
}
object RPCPair {
  type Aux[A, B] = RPCPair[A] {
    type Response = B
  }
}
