package io.iohk.metronome.examples.robot.models

import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.examples.robot.codecs.RobotCodecs
import scodec.bits.ByteVector

case class RobotBlock(
    parentHash: Hash,
    postStateHash: Hash,
    command: Robot.Command
) {
  lazy val hash: Hash = codecHash(this)(RobotCodecs.blockCodec)
}

object RobotBlock {
  def genesis(row: Int, col: Int, orientation: Robot.Orientation): RobotBlock =
    RobotBlock(
      parentHash = Hash(ByteVector.empty),
      postStateHash = Robot
        .State(
          position = Robot.Position(
            row = row,
            col = col
          ),
          orientation = orientation
        )
        .hash,
      command = Robot.Command.Rest
    )
}
