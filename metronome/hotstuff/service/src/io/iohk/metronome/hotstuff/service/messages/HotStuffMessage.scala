package io.iohk.metronome.hotstuff.service.messages

import io.iohk.metronome.hotstuff
import io.iohk.metronome.hotstuff.consensus.basic.Agreement

/** Messages which are generic to any HotStuff BFT agreement. */
sealed trait HotStuffMessage[A <: Agreement]

object HotStuffMessage {

  /** Messages which are part of the basic HotStuff BFT algorithm itself. */
  case class ConsensusMessage[A <: Agreement](
      message: hotstuff.consensus.basic.Message[A]
  ) extends HotStuffMessage[A]

  /** Messages that support the HotStuff BFT agreement but aren't part of
    * the core algorithm, e.g. block and view number synchronisation.
    */
  case class SyncMessage[A <: Agreement](
      message: hotstuff.service.messages.SyncMessage[A]
  ) extends HotStuffMessage[A]

}
