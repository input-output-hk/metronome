package io.iohk.metronome.examples.robot.models

import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.examples.robot.codecs.RobotCodecs

case class RobotBlock(
    parentHash: Hash,
    height: Long,
    postStateHash: Hash,
    command: Robot.Command
) {
  lazy val hash: Hash = codecHash(this)(RobotCodecs.blockCodec)
}
