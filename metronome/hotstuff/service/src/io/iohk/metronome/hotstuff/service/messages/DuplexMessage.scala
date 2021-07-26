package io.iohk.metronome.hotstuff.service.messages

import io.iohk.metronome.hotstuff
import io.iohk.metronome.hotstuff.consensus.basic.Agreement

/** Messages type to use in the networking layer if the use case has
  * application specific message types, e.g. ledger synchronisation,
  * not just the general BFT agreement (which could be enough if
  * we need to execute all blocks to synchronize state).
  */
sealed trait DuplexMessage[+A <: Agreement, +M]

object DuplexMessage {

  /** General BFT agreement message. */
  case class AgreementMessage[A <: Agreement](
      message: hotstuff.service.messages.HotStuffMessage[A]
  ) extends DuplexMessage[A, Nothing]

  /** Application specific message. */
  case class ApplicationMessage[M](
      message: M
  ) extends DuplexMessage[Nothing, M]
}
