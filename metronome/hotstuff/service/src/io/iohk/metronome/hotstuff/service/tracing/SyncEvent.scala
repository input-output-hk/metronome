package io.iohk.metronome.hotstuff.service.tracing

import io.iohk.metronome.hotstuff.consensus.basic.Agreement

sealed trait SyncEvent[+A <: Agreement]

object SyncEvent {

  /** A federation member is sending us so many requests that its work queue is full. */
  case class QueueFull[A <: Agreement](publicKey: A#PKey) extends SyncEvent[A]

  /** An unexpected error in one of the background tasks. */
  case class Error(error: Throwable) extends SyncEvent[Nothing]
}
