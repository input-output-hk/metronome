package io.iohk.metronome.hotstuff.consensus.basic

import io.iohk.ethereum.crypto.ECDSASignature
import io.iohk.metronome.crypto.hash.Keccak256
import io.iohk.metronome.crypto.{
  ECPrivateKey,
  ECPublicKey,
  GroupSignature,
  PartialSignature
}
import io.iohk.metronome.hotstuff.consensus.basic.Signing.{GroupSig, PartialSig}
import io.iohk.metronome.hotstuff.consensus.{Federation, ViewNumber}
import scodec.bits.ByteVector

/** Facilitates a Secp256k1 elliptic curve signing scheme using
  * `io.iohk.ethereum.crypto.ECDSASignature`
  * A group signature is simply a concatenation (sequence) of partial signatures
  */
class Secp256k1Signing[A <: Secp256k1Agreement](
    contentSerializer: (VotingPhase, ViewNumber, A#Hash) => ByteVector
) extends Signing[A] {

  override def sign(
      signingKey: ECPrivateKey,
      phase: VotingPhase,
      viewNumber: ViewNumber,
      blockHash: A#Hash
  ): PartialSig[A] = {
    val msgHash = contentHash(phase, viewNumber, blockHash)
    PartialSignature(ECDSASignature.sign(msgHash, signingKey.underlying))
  }

  override def combine(
      signatures: Seq[PartialSig[A]]
  ): GroupSig[A] =
    GroupSignature(signatures.map(_.sig).toList)

  /** Validate that partial signature was created by a given public key.
    *
    * Check that the signer is part of the federation.
    */
  override def validate(
      publicKey: ECPublicKey,
      signature: PartialSig[A],
      phase: VotingPhase,
      viewNumber: ViewNumber,
      blockHash: A#Hash
  ): Boolean = {
    val msgHash = contentHash(phase, viewNumber, blockHash)
    signature.sig
      .publicKey(msgHash)
      .map(ECPublicKey(_))
      .contains(publicKey)
  }

  /** Validate a group signature.
    *
    * Check that enough members of the federation signed,
    * and only the members.
    */
  override def validate(
      federation: Federation[ECPublicKey],
      signature: GroupSig[A],
      phase: VotingPhase,
      viewNumber: ViewNumber,
      blockHash: A#Hash
  ): Boolean = {
    val msgHash = contentHash(phase, viewNumber, blockHash)
    val signers =
      signature.sig
        .flatMap(s => s.publicKey(msgHash).map(ECPublicKey(_)))
        .toSet

    val areUniqueSigners     = signers.size == signature.sig.size
    val areFederationMembers = (signers -- federation.publicKeys).isEmpty
    val isQuorumReached      = signers.size == federation.quorumSize

    areUniqueSigners && areFederationMembers && isQuorumReached
  }

  private def contentHash(
      phase: VotingPhase,
      viewNumber: ViewNumber,
      blockHash: A#Hash
  ): Array[Byte] =
    Keccak256(contentSerializer(phase, viewNumber, blockHash)).toArray
}
