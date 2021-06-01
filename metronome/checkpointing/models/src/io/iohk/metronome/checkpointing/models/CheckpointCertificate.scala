package io.iohk.metronome.checkpointing.models

import cats.data.NonEmptyList
import io.iohk.metronome.hotstuff.consensus.basic.{QuorumCertificate, Phase}
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.checkpointing.models.Transaction.CheckpointCandidate

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
  def construct(
      block: Block,
      headers: NonEmptyList[Block.Header],
      commitQC: QuorumCertificate[CheckpointingAgreement, Phase.Commit]
  ): Option[CheckpointCertificate] =
    constructProof(block).map { case (proof, cp) =>
      CheckpointCertificate(headers, cp, proof, commitQC)
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
