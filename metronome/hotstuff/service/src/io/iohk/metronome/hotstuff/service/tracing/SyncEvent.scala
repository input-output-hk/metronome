package io.iohk.metronome.hotstuff.service.tracing

import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.hotstuff.service.messages.SyncMessage

sealed trait SyncEvent[+A <: Agreement]

object SyncEvent {

  /** A federation member is sending us so many requests that its work queue is full. */
  case class QueueFull[A <: Agreement](publicKey: A#PKey) extends SyncEvent[A]

  /** A response was ignored either because the request ID didn't match, or it already timed out,
    * or the response type didn't match the expected one based on the request.
    */
  case class ResponseIgnored[A <: Agreement](
      response: SyncMessage[A] with SyncMessage.Response
  ) extends SyncEvent[A]

  /** An unexpected error in one of the background tasks. */
  case class Error(error: Throwable) extends SyncEvent[Nothing]
}
