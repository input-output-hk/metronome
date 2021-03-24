package metronome.hotstuff.consensus.basic

import metronome.crypto.GroupSignature
import metronome.hotstuff.consensus.ViewNumber

/** A Quorum Certifcate (QC) over a tuple (message-type, view-number, block-hash) is a data type
  * that combines a collection of signatures for the same tuple signed by (n − f) replicas.
  */
case class QuorumCertificate[A <: Agreement](
    phase: VotingPhase,
    viewNumber: ViewNumber,
    blockHash: A#Hash,
    signature: GroupSignature[A#PKey, (VotingPhase, ViewNumber, A#Hash), A#GSig]
)
