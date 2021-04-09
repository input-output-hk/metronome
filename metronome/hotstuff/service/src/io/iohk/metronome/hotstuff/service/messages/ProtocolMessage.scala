package io.iohk.metronome.hotstuff.service.messages

import io.iohk.metronome.hotstuff
import io.iohk.metronome.hotstuff.consensus.basic.Agreement

/** Messages that comprise the HotStuff BFT agreement. */
sealed trait ProtocolMessage[A <: Agreement]

object ProtocolMessage {

  /** Messages which are part of the basic HotStuff BFT algorithm itself. */
  case class ConsensusMessage[A <: Agreement](
      message: hotstuff.consensus.basic.Message[A]
  ) extends ProtocolMessage[A]

  /** Messages that support the HotStuff BFT agreement but aren't part of
    * the core algorithm, e.g. block and view number synchronisation.
    */
  case class SyncMessage[A <: Agreement](
      message: hotstuff.service.messages.SyncMessage[A]
  ) extends ProtocolMessage[A]
}
