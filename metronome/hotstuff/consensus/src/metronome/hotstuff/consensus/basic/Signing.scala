package metronome.hotstuff.consensus.basic

import metronome.crypto.{PartialSignature, GroupSignature}
import metronome.hotstuff.consensus.ViewNumber

trait Signing[A <: Agreement] {

  def sign(
      signingKey: A#SKey,
      phase: VotingPhase,
      viewNumber: ViewNumber,
      blockHash: A#Hash
  ): Signing.PartialSig[A]

  def combine(
      signatures: Seq[Signing.PartialSig[A]]
  ): Signing.GroupSig[A]

  /** Validate a partial signature.
    *
    * Check that the signer is part of the federation.
    */
  def validate(
      signature: Signing.PartialSig[A],
      phase: VotingPhase,
      viewNumber: ViewNumber,
      blockHash: A#Hash
  ): Boolean

  /** Validate a group signature.
    *
    * Check that enough members of the federation signed,
    * and only the members.
    */
  def validate(
      signature: Signing.GroupSig[A],
      phase: VotingPhase,
      viewNumber: ViewNumber,
      blockHash: A#Hash
  ): Boolean

  def validate(vote: Message.Vote[A]): Boolean =
    validate(vote.signature, vote.phase, vote.viewNumber, vote.blockHash)

  def validate(qc: QuorumCertificate[A]): Boolean =
    validate(qc.signature, qc.phase, qc.viewNumber, qc.blockHash)
}

object Signing {
  def apply[A <: Agreement: Signing]: Signing[A] = implicitly[Signing[A]]

  type PartialSig[A <: Agreement] =
    PartialSignature[A#PKey, (VotingPhase, ViewNumber, A#Hash), A#PSig]

  type GroupSig[A <: Agreement] =
    GroupSignature[A#PKey, (VotingPhase, ViewNumber, A#Hash), A#GSig]
}
