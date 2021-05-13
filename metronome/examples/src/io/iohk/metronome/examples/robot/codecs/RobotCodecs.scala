package io.iohk.metronome.examples.robot.codecs

import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.hotstuff.service.codecs._
import io.iohk.metronome.examples.robot.RobotAgreement
import io.iohk.metronome.examples.robot.models.{Robot, RobotBlock}
import io.iohk.metronome.examples.robot.service.messages.RobotMessage
import scodec.Codec
import scodec.codecs._
import scodec.bits.ByteVector

object RobotCodecs
    extends DefaultConsensusCodecs[RobotAgreement]
    with DefaultProtocolCodecs[RobotAgreement]
    with DefaultSecp256k1Codecs[RobotAgreement]
    with DefaultMessageCodecs[RobotAgreement]
    with DefaultDuplexMessageCodecs[RobotAgreement, RobotMessage] {
  import scodec.codecs.implicits._

  override implicit val hashCodec: Codec[Hash] =
    Codec[ByteVector].xmap(Hash(_), identity)

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
  implicit val robotPositionCodec: Codec[Robot.Position] =
    Codec.deriveLabelledGeneric

  implicit val robotOrientationCodec: Codec[Robot.Orientation] = {
    import Robot.Orientation._
    mappedEnum(uint4, North -> 1, East -> 2, South -> 3, West -> 4)
  }

  implicit val robotStateCodec: Codec[Robot.State] =
    Codec.deriveLabelledGeneric

  override implicit val blockCodec: Codec[RobotBlock] =
    Codec.deriveLabelledGeneric

  implicit val getStateRequestCodec: Codec[RobotMessage.GetStateRequest] =
    Codec.deriveLabelledGeneric

  implicit val getStateResponseCodec: Codec[RobotMessage.GetStateResponse] =
    Codec.deriveLabelledGeneric

  override implicit val applicationMessageCodec: Codec[RobotMessage] =
    discriminated[RobotMessage]
      .by(uint4)
      .typecase(0, getStateRequestCodec)
      .typecase(1, getStateResponseCodec)
}
