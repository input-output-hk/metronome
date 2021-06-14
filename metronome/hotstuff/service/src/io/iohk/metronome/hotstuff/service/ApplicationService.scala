package io.iohk.metronome.hotstuff.service

import cats.data.{NonEmptyVector, NonEmptyList}
import io.iohk.metronome.hotstuff.consensus.basic.{Agreement, QuorumCertificate}

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
  // I cannot be sure that all blocks that get committed to gether fit into memory,
  // so we pass them one by one, but all of them are accompanied by the final Commit Q.C.
  // and the path of block hashes from the block being executed to the one committed.
  // Perhaps the application service can cache the headers if it needs to produce a
  // proof of the BFT agreement at the end.
  // Returns a flag to indicate whether the block execution results have been persisted,
  // whether the block and any corresponding state can be used as a starting point after a restart.
  def executeBlock(
      block: A#Block,
      commitQC: QuorumCertificate[A],
      commitPath: NonEmptyList[A#Hash]
  ): F[Boolean]

  // TODO (PM-3135): Tell the application to sync any state of the block, i.e. the Ledger.
  // The `sources` are peers who most probably have this state.
  // The full `block` is given because it may not be persisted yet.
  def syncState(sources: NonEmptyVector[A#PKey], block: A#Block): F[Unit]
}
