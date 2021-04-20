package io.iohk.metronome.hotstuff.service

import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.hotstuff.consensus.basic.QuorumCertificate

/** Status has all the fields necessary for nodes to sync with each other.
  *
  * This is to facilitate nodes rejoining the network,
  * or re-syncing their views after some network glitch.
  */
case class Status[A <: Agreement](
    viewNumber: ViewNumber,
    prepareQC: QuorumCertificate[A],
    commitQC: QuorumCertificate[A]
)
