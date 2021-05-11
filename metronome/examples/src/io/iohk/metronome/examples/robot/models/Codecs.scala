package io.iohk.metronome.examples.robot.models

import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.hotstuff.consensus.basic.{Phase, VotingPhase}
import scodec.Codec
import scodec.bits.{ByteVector}
import scodec.codecs._
import io.iohk.metronome.hotstuff.consensus.basic.Phase
import io.iohk.metronome.hotstuff.consensus.ViewNumber

// See https://github.com/scodec/scodec/blob/v1.11.7/unitTests/src/test/scala/scodec/codecs/DiscriminatorCodecTest.scala
object Codecs {
  import scodec.codecs.implicits._

  implicit val hashCodec: Codec[Hash] =
    implicitly[Codec[ByteVector]].xmap(Hash(_), identity)

  implicit val commandCodec: Codec[Robot.Command] = {
    import Robot.Command._
    mappedEnum(
      uint8,
      Rest        -> 1,
      MoveForward -> 2,
      TurnLeft    -> 3,
      TurnRight   -> 4
    )
  }

  implicit val robotBlockCodec: Codec[RobotBlock] =
    Codec.deriveLabelledGeneric

  implicit val robotPositionCodec: Codec[Robot.Position] =
    Codec.deriveLabelledGeneric

  implicit val robotOrientationCodec: Codec[Robot.Orientation] = {
    import Robot.Orientation._
    mappedEnum(uint8, North -> 1, East -> 2, South -> 3, West -> 4)
  }

  implicit val robotStateCodec: Codec[Robot.State] =
    Codec.deriveLabelledGeneric

  implicit val phaseCodec: Codec[VotingPhase] = {
    import Phase._
    mappedEnum(uint8, Prepare -> 1, PreCommit -> 2, Commit -> 3)
  }

  implicit val viewNumberCodec: Codec[ViewNumber] =
    implicitly[Codec[Long]].xmap(ViewNumber(_), identity)

  implicit val contentCodec: Codec[(VotingPhase, ViewNumber, Hash)] =
    phaseCodec ~~ viewNumberCodec ~~ hashCodec

}
