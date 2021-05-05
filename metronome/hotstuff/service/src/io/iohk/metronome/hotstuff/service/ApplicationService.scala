package io.iohk.metronome.hotstuff.service

import cats.data.NonEmptyVector
import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.hotstuff.consensus.basic.QuorumCertificate

/** Represents the "application" domain to the HotStuff module,
  * performing all delegations that HotStuff can't do on its own.
  */
trait ApplicationService[F[_], A <: Agreement] {
  // TODO (PM-3109): Create block.
  def createBlock(highQC: QuorumCertificate[A]): F[Option[A#Block]]

  // TODO (PM-3132, PM-3133): Block validation.
  // Returns None if validation cannot be carried out due to data availability issues within a given timeout.
  def validateBlock(block: A#Block): F[Option[Boolean]]

  // TODO (PM-3108, PM-3107, PM-3137, PM-3110): Tell the application to execute a block.
  def executeBlock(block: A#Block): F[Unit]

  // TODO (PM-3135): Tell the application to sync any state of the block, i.e. the Ledger.
  // The `sources` are peers who most probably have this state.
  // The full `block` is given because it may not be persisted yet.
  def syncState(sources: NonEmptyVector[A#PKey], block: A#Block): F[Unit]
}
