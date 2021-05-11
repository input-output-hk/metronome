package io.iohk.metronome.examples.robot.models

import io.iohk.metronome.crypto.hash.{Hash, Keccak256}

case class RobotBlock(
    parentHash: Hash,
    command: Robot.Command
) {
  lazy val hash: Hash =
    Keccak256(Codecs.robotBlockCodec.encode(this).require)
}
