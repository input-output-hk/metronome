package io.iohk.metronome.checkpointing

import io.iohk.metronome.hotstuff.consensus
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  Secp256k1Agreement
}

object CheckpointingAgreement extends Secp256k1Agreement {
  override type Block = models.Block
  override type Hash  = models.Block.Hash

  implicit val block: consensus.basic.Block[CheckpointingAgreement] =
    new consensus.basic.Block[CheckpointingAgreement] {
      override def blockHash(b: models.Block) =
        b.hash
      override def parentBlockHash(b: models.Block) =
        b.header.parentHash
      override def height(b: Block): Long =
        b.header.height
      override def isValid(b: models.Block) =
        models.Block.isValid(b)
    }

  type GroupSignature = Agreement.GroupSignature[CheckpointingAgreement]
}
