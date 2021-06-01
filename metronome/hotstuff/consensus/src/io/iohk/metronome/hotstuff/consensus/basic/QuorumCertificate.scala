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

  /** In protocol messages we can treat QCs as `QuorumCertificate[A, VotingPhase]`,
    * and coerce to a specific type after checking what it is. We can also coerce
    * back into the supertype, if necessary.
    */
  def coerce[V <: VotingPhase](implicit
      ct: ClassTag[V]
  ): QuorumCertificate[A, V] = {
    assert(
      ct.unapply(phase).isDefined,
      s"Can only coerce between VotingPhase and a subclass; attempted to cast ${phase} to ${ct.runtimeClass.getSimpleName}"
    )
    this.asInstanceOf[QuorumCertificate[A, V]]
  }

  // The following methods are used in tests.

  def withPhase[V <: VotingPhase](phase: V) =
    copy[A, V](phase = phase)

  def withViewNumber(viewNumber: ViewNumber) =
    copy[A, P](viewNumber = viewNumber)

  def withBlockHash(blockHash: A#Hash) =
    copy[A, P](blockHash = blockHash)

  def withSignature(
      signature: GroupSignature[
        A#PKey,
        (VotingPhase, ViewNumber, A#Hash),
        A#GSig
      ]
  ) =
    copy[A, P](signature = signature)

  // Sometimes when we have just `QuorumCertificate[A, _]` the compiler
  // can't prove that `.phase` is a `VotingPhase` and not just `$1`.
  def votingPhase: VotingPhase = phase
}
