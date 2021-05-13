package io.iohk.metronome.examples.robot.codecs

import scodec.Codec
import scodec.codecs._
import io.iohk.metronome.examples.robot.models._

// See https://github.com/scodec/scodec/blob/v1.11.7/unitTests/src/test/scala/scodec/codecs/DiscriminatorCodecTest.scala
trait RobotModelCodecs { self: RobotConsensusCodecs =>
  import scodec.codecs.implicits._

  implicit val commandCodec: Codec[Robot.Command] = {
    import Robot.Command._
    mappedEnum(
      uint4,
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
    mappedEnum(uint4, North -> 1, East -> 2, South -> 3, West -> 4)
  }

  implicit val robotStateCodec: Codec[Robot.State] =
    Codec.deriveLabelledGeneric
}
