package io.iohk.metronome.checkpointing.models

import io.iohk.ethereum.rlp
import io.iohk.metronome.hotstuff.consensus.{Federation, ViewNumber}
import io.iohk.metronome.hotstuff.consensus.basic.{
  Secp256k1Signing,
  VotingPhase,
  Signing
}
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.checkpointing.models.RLPCodecs._
import scodec.bits.ByteVector

class CheckpointingSigning(
    genesisHash: Block.Hash
) extends Secp256k1Signing[CheckpointingAgreement]((phase, viewNumber, hash) =>
      ByteVector(
        rlp.encode(phase) ++ rlp.encode(viewNumber) ++ rlp.encode(hash)
      )
    ) {

  /** Override quorum certificate validation rule so we accept the quorum
    * certificate we can determinsiticially fabricate without a group signature.
    */
  override def validate(
      federation: Federation[CheckpointingAgreement.PKey],
      signature: Signing.GroupSig[CheckpointingAgreement],
      phase: VotingPhase,
      viewNumber: ViewNumber,
      blockHash: CheckpointingAgreement.Hash
  ): Boolean =
    if (blockHash == genesisHash) {
      signature.sig.isEmpty
    } else {
      super.validate(
        federation,
        signature,
        phase,
        viewNumber,
        blockHash
      )
    }
}
