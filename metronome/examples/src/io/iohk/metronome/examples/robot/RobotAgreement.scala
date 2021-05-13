package io.iohk.metronome.examples.robot

import io.iohk.metronome.crypto
import io.iohk.metronome.hotstuff.consensus
import io.iohk.metronome.hotstuff.consensus.basic.{Secp256k1Agreement, Signing}
import io.iohk.metronome.examples.robot.models.RobotBlock

object RobotAgreement extends Secp256k1Agreement {
  override type Block = RobotBlock
  override type Hash  = crypto.hash.Hash

  implicit val block: consensus.basic.Block[RobotAgreement] =
    new consensus.basic.Block[RobotAgreement] {
      override def blockHash(b: RobotBlock)       = b.hash
      override def parentBlockHash(b: RobotBlock) = b.parentHash
      override def isValid(b: RobotBlock)         = true
    }

  implicit val signing: Signing[RobotAgreement] =
    Signing.secp256k1((phase, viewNumber, hash) =>
      codecs.RobotCodecs.contentCodec
        .encode((phase, viewNumber, hash))
        .require
        .toByteVector
    )

}
