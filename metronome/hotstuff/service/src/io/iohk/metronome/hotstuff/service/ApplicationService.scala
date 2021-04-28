package io.iohk.metronome.hotstuff.service

import cats.data.NonEmptyVector
import io.iohk.metronome.hotstuff.consensus.basic.Agreement

/** Represents the "application" domain to the HotStuff module,
  * performing all delegations that HotStuff can't do on its own.
  */
trait ApplicationService[F[_], A <: Agreement] {
  // TODO (PM-3132, PM-3133): Block validation.
  def validateBlock(block: A#Block): F[Boolean]

  // TODO (PM-3135): Tell the application to sync any state of the block, i.e. the Ledger.
  // The `sources` are peers who most probably have this state.
  // The full `block` is given because it may not be persisted yet.
  def syncState(sources: NonEmptyVector[A#PKey], block: A#Block): F[Unit]
}
