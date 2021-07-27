package io.iohk.metronome.examples.robot.models

import io.iohk.metronome.examples.robot.RobotAgreement
import io.iohk.metronome.examples.robot.codecs.RobotCodecs
import io.iohk.metronome.hotstuff.consensus.Federation
import io.iohk.metronome.hotstuff.consensus.basic.{
  QuorumCertificate,
  Secp256k1Signing
}
import io.iohk.metronome.hotstuff.consensus.basic.VotingPhase

class RobotSigning(
    genesisHash: RobotAgreement.Hash
) extends Secp256k1Signing[RobotAgreement]((phase, viewNumber, hash) =>
      RobotCodecs.contentCodec
        .encode((phase, viewNumber, hash))
        .require
        .toByteVector
    ) {

  /** Override quorum certificate validation rule so we accept the quorum
    * certificate we can determinsitcially fabricate without a group signature.
    */
  override def validate(
      federation: Federation[RobotAgreement.PKey],
      quorumCertificate: QuorumCertificate[RobotAgreement, VotingPhase]
  ): Boolean =
    if (quorumCertificate.blockHash == genesisHash) {
      quorumCertificate.signature.sig.isEmpty
    } else {
      super.validate(federation, quorumCertificate)
    }
}
