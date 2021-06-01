package io.iohk.metronome.hotstuff.consensus.basic

import io.iohk.metronome.crypto.GroupSignature
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import scala.reflect.ClassTag

/** A Quorum Certifcate (QC) over a tuple (message-type, view-number, block-hash) is a data type
  * that combines a collection of signatures for the same tuple signed by (n − f) replicas.
  */
case class QuorumCertificate[A <: Agreement, +P <: VotingPhase](
    phase: P,
    viewNumber: ViewNumber,
    blockHash: A#Hash,
    signature: GroupSignature[A#PKey, (VotingPhase, ViewNumber, A#Hash), A#GSig]
) {
  def coerce[V <: VotingPhase](implicit
      ct: ClassTag[V]
  ): QuorumCertificate[A, V] = {
    // assert(ct.unapply(phase).isDefined) // Not always true in testing.
    this.asInstanceOf[QuorumCertificate[A, V]]
  }
  protected[basic] def withPhase[V <: VotingPhase](phase: V) =
    copy[A, V](phase = phase)

  protected[basic] def withViewNumber(viewNumber: ViewNumber) =
    copy[A, P](viewNumber = viewNumber)

  protected[basic] def withBlockHash(blockHash: A#Hash) =
    copy[A, P](blockHash = blockHash)

  protected[basic] def withSignature(
      signature: GroupSignature[
        A#PKey,
        (VotingPhase, ViewNumber, A#Hash),
        A#GSig
      ]
  ) =
    copy[A, P](signature = signature)

  // Sometimes when we have just `QuorumCertificate[A, _]` the compiler
  // can't prove that `.phase` is a `VotingPhase` and not just `$1`.
  protected[basic] def votingPhase: VotingPhase = phase
}
