package io.iohk.metronome.checkpointing

import io.iohk.metronome.crypto
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.{
  Secp256k1Agreement,
  Signing,
  VotingPhase
}
import scodec.bits.ByteVector
import io.iohk.ethereum.rlp
import io.iohk.metronome.checkpointing.models.RLPCodecs._

object CheckpointingAgreement extends Secp256k1Agreement {
  override type Block = models.Block
  override type Hash  = models.Block.Header.Hash

  type GroupSignature = crypto.GroupSignature[
    PKey,
    (VotingPhase, ViewNumber, Hash),
    GSig
  ]

  implicit val signing: Signing[CheckpointingAgreement] =
    Signing.secp256k1((phase, viewNumber, hash) =>
      ByteVector(
        rlp.encode(phase) ++ rlp.encode(viewNumber) ++ rlp.encode(hash)
      )
    )
}
