package io.iohk.metronome.checkpointing

import io.iohk.ethereum.crypto.ECDSASignature
import io.iohk.metronome.crypto
import io.iohk.metronome.crypto.{ECPrivateKey, ECPublicKey}
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.{Agreement, VotingPhase}
import scodec.bits.ByteVector
import io.iohk.ethereum.rlp
import io.iohk.metronome.checkpointing.models.RLPCodecs._

object CheckpointingAgreement extends Agreement {
  override type Block = models.Block
  override type Hash  = models.Block.Header.Hash
  override type PSig  = ECDSASignature
  // TODO (PM-2935): Replace list with theshold signatures.
  override type GSig = List[ECDSASignature]
  override type PKey = ECPublicKey
  override type SKey = ECPrivateKey

  type GroupSignature = crypto.GroupSignature[
    PKey,
    (VotingPhase, ViewNumber, Hash),
    GSig
  ]

  def signedContentSerialiser(
      phase: VotingPhase,
      viewNumber: ViewNumber,
      hash: Hash
  ): ByteVector =
    ByteVector(rlp.encode(phase) ++ rlp.encode(viewNumber) ++ rlp.encode(hash))
}
