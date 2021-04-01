package io.iohk.metronome.checkpointing.models

import cats.data.NonEmptyList
import io.iohk.metronome.hotstuff.consensus.basic.QuorumCertificate
import io.iohk.metronome.checkpointing.CheckpointingAgreement

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
    // `head` is the `Block.Header` that has the Commit Q.C.;
    // `last` is the `Block.Header` that had the `CheckpointCandiate` in its `Body`.
    headers: NonEmptyList[Block.Header],
    // The opaque contents of the checkpoint that has been agreed upon.
    checkpoint: Transaction.CheckpointCandidate,
    // Proof that `checkpoint` is part of `headers.last.contentMerkleRoot`.
    proof: MerkleTree.Proof,
    // Commit Q.C. over `headers.head`.
    commitQC: QuorumCertificate[CheckpointingAgreement]
)
