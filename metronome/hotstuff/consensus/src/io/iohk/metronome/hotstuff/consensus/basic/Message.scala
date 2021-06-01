package io.iohk.metronome.hotstuff.consensus.basic

import io.iohk.metronome.crypto.PartialSignature
import io.iohk.metronome.hotstuff.consensus.ViewNumber

/** Basic HotStuff protocol messages. */
sealed trait Message[A <: Agreement] {

  /** Messages are only accepted if they match the node's current view number. */
  def viewNumber: ViewNumber
}

/** Message from the leader to the replica. */
sealed trait LeaderMessage[A <: Agreement] extends Message[A]

/** Message from the replica to the leader. */
sealed trait ReplicaMessage[A <: Agreement] extends Message[A]

object Message {

  /** The leader proposes a new block in the `Prepare` phase,
    * using the High Q.C. gathered from `NewView` messages.
    */
  case class Prepare[A <: Agreement](
      viewNumber: ViewNumber,
      block: A#Block,
      highQC: QuorumCertificate[A, Phase.Prepare]
  ) extends LeaderMessage[A]

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

  /** Having collected enough votes from replicas,
    * the leader combines the votes into a Q.C. and
    * broadcasts it to replicas:
    *  - Prepare votes combine into a Prepare Q.C., expected in the PreCommit phase.
    *  - PreCommit votes combine into a PreCommit Q.C., expected in the Commit phase.
    *  - Commit votes combine into a Commit Q.C, expected in the Decide phase.
    *
    * The certificate contains the hash of the block to vote on.
    */
  case class Quorum[A <: Agreement, P <: VotingPhase](
      viewNumber: ViewNumber,
      quorumCertificate: QuorumCertificate[A, P]
  ) extends LeaderMessage[A]

  /** At the end of the round, replicas send the `NewView` message
    * to the next leader with the last Prepare Q.C.
    */
  case class NewView[A <: Agreement](
      viewNumber: ViewNumber,
      prepareQC: QuorumCertificate[A, Phase.Prepare]
  ) extends ReplicaMessage[A]
}
