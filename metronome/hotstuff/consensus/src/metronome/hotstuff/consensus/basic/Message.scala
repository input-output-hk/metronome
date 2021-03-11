package metronome.hotstuff.consensus.basic

import metronome.crypto.PartialSignature
import metronome.hotstuff.consensus.ViewNumber

/** Basic HotStuff protocol messages. */
sealed trait Message[A <: Agreement] {

  /** Messages are only accepted if they match the node's current view number. */
  def viewNumber: ViewNumber
  def phase: Phase
}

/** Message from the leader to the replica. */
sealed trait LeaderMessage[A <: Agreement] extends Message[A]

/** Message carrying a Quorum Certificate. */
sealed trait QuorumMessage[A <: Agreement] extends LeaderMessage[A] {
  def certificate: QuorumCertificate[A]
}

/** Message from the replica to the leader. */
sealed trait ReplicaMessage[A <: Agreement] extends Message[A]

object Message {

  /** The leader proposes a new block in the `Prepare` phase,
    * using the High Q.C. gathered from `NewView` messages.
    */
  case class Prepare[A <: Agreement](
      viewNumber: ViewNumber,
      block: A#Block,
      highQC: QuorumCertificate[A]
  ) extends LeaderMessage[A] {
    override val phase: Phase = Phase.Prepare
  }

  /** Having collected enough `Prepare` votes from replicas,
    * the leader combines the votes into a Prepare Q.C. and
    * broadcasts it to replicas in a `PreCommit` message.
    *
    * The certificate contains the hash of the block to vote on.
    */
  case class PreCommit[A <: Agreement](
      viewNumber: ViewNumber,
      prepareQC: QuorumCertificate[A]
  ) extends QuorumMessage[A] {
    override val phase: Phase                      = Phase.PreCommit
    override val certificate: QuorumCertificate[A] = prepareQC
  }

  /** Having collected enough `PreCommit` votes from replicas,
    * the leader combines the votes into a Pre-Commit Q.C. and
    * broadcasts it to replicas in a `Commit` message.
    *
    * The certificate contains the hash of the block to vote on.
    */
  case class Commit[A <: Agreement](
      viewNumber: ViewNumber,
      precommitQC: QuorumCertificate[A]
  ) extends QuorumMessage[A] {
    override val phase: Phase                      = Phase.Commit
    override val certificate: QuorumCertificate[A] = precommitQC
  }

  /** Having collected enough `Commit` votes from replicas,
    * the leader combines the votes into a Commit Q.C. and
    * broadcasts it to replicas in a `Decide` message.
    *
    * The certificate contains the hash of the block to execute.
    */
  case class Decide[A <: Agreement](
      viewNumber: ViewNumber,
      commitQC: QuorumCertificate[A]
  ) extends QuorumMessage[A] {
    override val phase: Phase                      = Phase.Decide
    override val certificate: QuorumCertificate[A] = commitQC
  }

  /** Having received one of the leader messages, the replica
    * casts its vote with its partical signature.
    *
    * The vote carries either the hash of the block, which
    * was either received full in the `Prepare` message,
    * or as part of a `QuorumCertificate`.
    */
  case class Vote[A <: Agreement](
      viewNumber: ViewNumber,
      phase: VotingPhase,
      blockHash: A#Hash,
      signature: PartialSignature[
        A#PKey,
        (VotingPhase, ViewNumber, A#Hash),
        A#PSig
      ]
  ) extends ReplicaMessage[A]

  /** At the end of the round, replicas send the `NewView` message
    * to the next leader with the last Prepare Q.C.
    */
  case class NewView[A <: Agreement](
      viewNumber: ViewNumber,
      prepareQC: QuorumCertificate[A]
  ) extends ReplicaMessage[A] {
    override def phase: Phase = Phase.Prepare
  }
}
