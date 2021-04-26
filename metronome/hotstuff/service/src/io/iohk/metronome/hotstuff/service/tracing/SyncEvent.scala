package io.iohk.metronome.hotstuff.service.tracing

import io.iohk.metronome.core.Validated
import io.iohk.metronome.hotstuff.consensus.basic.{Agreement, ProtocolError}
import io.iohk.metronome.hotstuff.service.messages.SyncMessage
import io.iohk.metronome.hotstuff.service.Status
import io.iohk.metronome.hotstuff.consensus.basic.ProtocolError

sealed trait SyncEvent[+A <: Agreement]

object SyncEvent {

  /** A federation member is sending us so many requests that its work queue is full. */
  case class QueueFull[A <: Agreement](
      sender: A#PKey
  ) extends SyncEvent[A]

  /** A request we sent couldn't be matched with a response in time. */
  case class RequestTimeout[A <: Agreement](
      recipient: A#PKey,
      request: SyncMessage[A] with SyncMessage.Request
  ) extends SyncEvent[A]

  /** A response was ignored either because the request ID didn't match, or it already timed out,
    * or the response type didn't match the expected one based on the request.
    */
  case class ResponseIgnored[A <: Agreement](
      sender: A#PKey,
      response: SyncMessage[A] with SyncMessage.Response,
      maybeError: Option[Throwable]
  ) extends SyncEvent[A]

  /** Performed a poll for `Status` across the federation.
    * Only contains results for federation members that responded within the timeout.
    */
  case class StatusPoll[A <: Agreement](
      statuses: Map[A#PKey, Validated[Status[A]]]
  ) extends SyncEvent[A]

  /** A federation members sent a `Status` with invalid content. */
  case class InvalidStatus[A <: Agreement](
      status: Status[A],
      error: ProtocolError.InvalidQuorumCertificate[A]
  ) extends SyncEvent[A]

  /** An unexpected error in one of the background tasks. */
  case class Error(error: Throwable) extends SyncEvent[Nothing]
}
