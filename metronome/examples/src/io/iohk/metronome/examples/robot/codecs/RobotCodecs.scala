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

  override implicit lazy val hashCodec: Codec[Hash] =
    Codec[ByteVector].xmap(Hash(_), identity)

  implicit lazy val commandCodec: Codec[Robot.Command] = {
    import Robot.Command._
    mappedEnum(
      uint2,
      Rest        -> 0,
      MoveForward -> 1,
      TurnLeft    -> 2,
      TurnRight   -> 3
    )
  }
  implicit lazy val robotPositionCodec: Codec[Robot.Position] =
    Codec.deriveLabelledGeneric

  implicit lazy val robotOrientationCodec: Codec[Robot.Orientation] = {
    import Robot.Orientation._
    mappedEnum(uint2, North -> 0, East -> 1, South -> 2, West -> 3)
  }

  implicit lazy val robotStateCodec: Codec[Robot.State] =
    Codec.deriveLabelledGeneric

  override implicit lazy val blockCodec: Codec[RobotBlock] =
    Codec.deriveLabelledGeneric

  implicit lazy val getStateRequestCodec: Codec[RobotMessage.GetStateRequest] =
    Codec.deriveLabelledGeneric

  implicit lazy val getStateResponseCodec
      : Codec[RobotMessage.GetStateResponse] =
    Codec.deriveLabelledGeneric

  override implicit lazy val applicationMessageCodec: Codec[RobotMessage] =
    discriminated[RobotMessage]
      .by(uint2)
      .typecase(0, getStateRequestCodec)
      .typecase(1, getStateResponseCodec)
}
