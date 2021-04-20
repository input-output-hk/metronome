package io.iohk.metronome.hotstuff.consensus.basic

import cats.evidence.Is
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
class Secp256k1Signing[A <: Agreement](
    contentSerialiser: (VotingPhase, ViewNumber, A#Hash) => ByteVector
)(implicit
    evPK: A#PKey Is ECPublicKey,
    evSK: A#SKey Is ECPrivateKey,
    evPS: A#PSig Is ECDSASignature,
    evGS: A#GSig Is List[ECDSASignature]
) extends Signing[A] {

  override def sign(
      signingKey: A#SKey,
      phase: VotingPhase,
      viewNumber: ViewNumber,
      blockHash: A#Hash
  ): PartialSig[A] = {
    val msgHash = Keccak256(
      contentSerialiser(phase, viewNumber, blockHash)
    ).toArray
    PartialSignature(ECDSASignature.sign(msgHash, signingKey.underlying))
  }

  override def combine(
      signatures: Seq[PartialSig[A]]
  ): GroupSig[A] =
    GroupSignature(signatures.map(_.sig).toList: List[ECDSASignature])

  /** Validate that partial signature was created by a given public key.
    *
    * Check that the signer is part of the federation.
    */
  override def validate(
      publicKey: A#PKey,
      signature: PartialSig[A],
      phase: VotingPhase,
      viewNumber: ViewNumber,
      blockHash: A#Hash
  ): Boolean = {
    val msgHash = Keccak256(
      contentSerialiser(phase, viewNumber, blockHash)
    ).toArray
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
      federation: Federation[A#PKey],
      signature: GroupSig[A],
      phase: VotingPhase,
      viewNumber: ViewNumber,
      blockHash: A#Hash
  ): Boolean = {
    val msgHash = Keccak256(
      contentSerialiser(phase, viewNumber, blockHash)
    ).toArray
    val signers =
      signature.sig
        .flatMap(s => s.publicKey(msgHash).map(ECPublicKey(_)))
        .toSet

    val areUniqueSigners     = signers.size == signature.sig.size
    val areFederationMembers = (signers -- federation.publicKeys).isEmpty
    val isQuorumReached      = signers.size >= federation.quorumSize

    areUniqueSigners && areFederationMembers && isQuorumReached
  }

  // For improved readability these provide implicit conversions
  // between the generic Agreement types and the concrete implementation
  // types based on the implicit type equality evidence
  private implicit val pKeyToConcrete: A#PKey => ECPublicKey = evPK.coerce
  private implicit def pKeyFToConcrete[F[_]]: F[A#PKey] => F[ECPublicKey] =
    evPK.substitute
  private implicit val sKeyToConcrete: A#SKey => ECPrivateKey = evSK.coerce
  private implicit val pSigFromConcrete: ECDSASignature => A#PSig =
    evPS.flip.coerce
  private implicit val pSigToConcrete: A#PSig => ECDSASignature = evPS.coerce
  private implicit def pSigFToConcrete[F[_]]: F[A#PSig] => F[ECDSASignature] =
    evPS.substitute
  private implicit val gSigToConcrete: A#GSig => List[ECDSASignature] =
    evGS.coerce
  private implicit val gSigFromConcrete: List[ECDSASignature] => A#GSig =
    evGS.flip.coerce
}
