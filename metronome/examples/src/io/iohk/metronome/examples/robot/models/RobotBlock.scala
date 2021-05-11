package io.iohk.metronome.examples.robot.models

import io.iohk.metronome.crypto.hash.Hash

case class RobotBlock(
    parentHash: Hash,
    postStateHash: Hash,
    command: Robot.Command
) {
  lazy val hash: Hash =
    codecHash(this)(Codecs.robotBlockCodec)
}
