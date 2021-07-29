package io.iohk.metronome.checkpointing.models

import cats.data.NonEmptyList
import io.iohk.metronome.hotstuff.consensus.basic.{QuorumCertificate, Phase}
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.checkpointing.models.Transaction.CheckpointCandidate
import io.iohk.metronome.hotstuff.consensus.basic.Signing
import io.iohk.metronome.hotstuff.consensus.Federation
import io.iohk.metronome.core.Validated

/** The Checkpoint Certificate is a proof of the BFT agreement
  * over a given Checkpoint Candidate.
  *
  * It contains the group signature over the block that the
  * federation committed, together with the sequence of blocks
  * from the one that originally introduced the Candidate.
  *
  * The interpreter can follow the parent-child relationships,
  * validate the hashes and the inclusion of the Candidate in
  * the original block, check the group signature, then unpack
  * the contents fo the Candidate to interpet it according to
  * whatever rules apply on the checkpointed PoW chain.
  */
case class CheckpointCertificate(
    // `head` is the `Block.Header` that had the `CheckpointCandidate` in its `Body`.
    // `last` is the `Block.Header` that has the Commit Q.C.;
    headers: NonEmptyList[Block.Header],
    // The opaque contents of the checkpoint that has been agreed upon.
    checkpoint: Transaction.CheckpointCandidate,
    // Proof that `checkpoint` is part of `headers.head.contentMerkleRoot`.
    proof: MerkleTree.Proof,
    // Commit Q.C. over `headers.last`.
    commitQC: QuorumCertificate[CheckpointingAgreement, Phase.Commit]
)

object CheckpointCertificate {

  /** Create a `CheckpointCertificate` from a `Block` that last had a `CheckpointCandidate`
    * and a list of `Block.Header`s leading up to the `QuorumCertifictate` that proves the
    * BFT agreement over the contents.
    */
  def construct(
      block: Block,
      headers: NonEmptyList[Block.Header],
      commitQC: QuorumCertificate[CheckpointingAgreement, Phase.Commit]
  ): Option[CheckpointCertificate] = {
    assert(block.hash == headers.head.hash)
    assert(commitQC.blockHash == headers.last.hash)

    constructProof(block).map { case (proof, cp) =>
      CheckpointCertificate(headers, cp, proof, commitQC)
    }
  }

  /** Validate a `CheckpointCertificate` by checking that:
    * - the chain of block headers is valid
    * - the quorum certificate is valid
    * - the Merkle proof of the candidate is valid
    */
  def validate(
      certificate: CheckpointCertificate,
      federation: Federation[CheckpointingAgreement.PKey]
  )(implicit
      signing: Signing[CheckpointingAgreement]
  ): Either[String, Validated[Transaction.CheckpointCandidate]] = {
    def check(
        message: String
    )(cond: Boolean): Either[String, Unit] =
      Either.cond(cond, (), message)

    for {
      _ <- check(
        "The headers do not correspond to a chain of parent-child blocks."
      ) {
        certificate.headers.toList.zip(certificate.headers.tail).forall {
          case (parent, child) =>
            parent.hash == child.parentHash
        }
      }

      _ <- check("The Commit Q.C. is not about the last block in the chain.") {
        certificate.commitQC.blockHash == certificate.headers.last.hash
      }

      _ <- check("The Commit Q.C. is invalid.") {
        signing.validate(federation, certificate.commitQC)
      }

      _ <- check("The Merkle proof is invalid.") {
        MerkleTree.verifyProof(
          certificate.proof,
          root = certificate.headers.head.contentMerkleRoot,
          leaf = MerkleTree.Hash(certificate.checkpoint.hash)
        )
      }
    } yield Validated[Transaction.CheckpointCandidate](certificate.checkpoint)
  }

  private def constructProof(
      block: Block
  ): Option[(MerkleTree.Proof, CheckpointCandidate)] =
    block.body.transactions.reverseIterator.collectFirst {
      case cp: CheckpointCandidate =>
        val txHashes =
          block.body.transactions.map(tx => MerkleTree.Hash(tx.hash))
        val tree   = MerkleTree.build(txHashes)
        val cpHash = MerkleTree.Hash(cp.hash)
        MerkleTree.generateProofFromHash(tree, cpHash).map(_ -> cp)
    }.flatten
}
