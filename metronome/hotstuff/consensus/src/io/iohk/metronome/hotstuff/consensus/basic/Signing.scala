package io.iohk.metronome.hotstuff.consensus.basic

import io.iohk.metronome.crypto.{GroupSignature, PartialSignature}
import io.iohk.metronome.hotstuff.consensus.{Federation, ViewNumber}
import scodec.bits.ByteVector

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

  /** Validate that partial signature was created by a given public key. */
  def validate(
      publicKey: A#PKey,
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
      federation: Federation[A#PKey],
      signature: Signing.GroupSig[A],
      phase: VotingPhase,
      viewNumber: ViewNumber,
      blockHash: A#Hash
  ): Boolean

  def validate(sender: A#PKey, vote: Message.Vote[A]): Boolean =
    validate(
      sender,
      vote.signature,
      vote.phase,
      vote.viewNumber,
      vote.blockHash
    )

  def validate(
      federation: Federation[A#PKey],
      quorumCertificate: QuorumCertificate[A]
  ): Boolean =
    validate(
      federation,
      quorumCertificate.signature,
      quorumCertificate.phase,
      quorumCertificate.viewNumber,
      quorumCertificate.blockHash
    )
}

object Signing {
  def apply[A <: Agreement: Signing]: Signing[A] = implicitly[Signing[A]]

  def secp256k1[A <: Secp256k1Agreement](
      contentSerializer: (VotingPhase, ViewNumber, A#Hash) => ByteVector
  ): Signing[A] = new Secp256k1Signing[A](contentSerializer)

  type PartialSig[A <: Agreement] =
    PartialSignature[A#PKey, (VotingPhase, ViewNumber, A#Hash), A#PSig]

  type GroupSig[A <: Agreement] =
    GroupSignature[A#PKey, (VotingPhase, ViewNumber, A#Hash), A#GSig]
}
