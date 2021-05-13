package io.iohk.metronome.hotstuff.consensus.basic

import io.iohk.metronome.crypto
import io.iohk.metronome.hotstuff.consensus.ViewNumber

/** Capture all the generic types in the BFT agreement,
  * so we don't have to commit to any particular set of content.
  */
trait Agreement {

  /** The container type that the agreement is about. */
  type Block

  /** The type we use for hashing blocks,
    * so they don't have to be sent in entirety in votes.
    */
  type Hash

  /** The concrete type that represents a partial signature. */
  type PSig

  /** The concrete type that represents a group signature. */
  type GSig

  /** The public key identity of federation members. */
  type PKey

  /** The secret key used for signing partial messages. */
  type SKey
}

object Agreement {
  // Convenience alias for groups signatures appearing in Quorum Certificates..
  type GroupSignature[A <: Agreement] = crypto.GroupSignature[
    A#PKey,
    (VotingPhase, ViewNumber, A#Hash),
    A#GSig
  ]

  // Convenience alias for groups signatures appearing in Votes.
  type PartialSignature[A <: Agreement] = crypto.PartialSignature[
    A#PKey,
    (VotingPhase, ViewNumber, A#Hash),
    A#PSig
  ]
}
