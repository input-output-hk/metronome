package io.iohk.metronome.examples.robot.codecs

import scodec.Codec
import scodec.codecs._
import io.iohk.metronome.examples.robot.RobotAgreement
import io.iohk.metronome.examples.robot.service.messages.RobotMessage
import io.iohk.metronome.hotstuff.service.messages.{
  DuplexMessage,
  SyncMessage,
  HotStuffMessage
}

trait RobotMessageCodecs {
  self: RobotConsensusCodecs with RobotModelCodecs with RobotProtocolCodecs =>
  import scodec.codecs.implicits._

  implicit val getStateRequestCodec: Codec[RobotMessage.GetStateRequest] =
    Codec.deriveLabelledGeneric

  implicit val getStateResponseCodec: Codec[RobotMessage.GetStateResponse] =
    Codec.deriveLabelledGeneric

  implicit val robotMessageCodec: Codec[RobotMessage] =
    discriminated[RobotMessage]
      .by(uint4)
      .typecase(0, getStateRequestCodec)
      .typecase(1, getStateResponseCodec)

  implicit val getStatusRequestCodec
      : Codec[SyncMessage.GetStatusRequest[RobotAgreement]] =
    Codec.deriveLabelledGeneric

  implicit val getStatusResponseCodec
      : Codec[SyncMessage.GetStatusResponse[RobotAgreement]] =
    Codec.deriveLabelledGeneric

  implicit val getBlockRequestCodec
      : Codec[SyncMessage.GetBlockRequest[RobotAgreement]] =
    Codec.deriveLabelledGeneric

  implicit val getBlockResponseCodec
      : Codec[SyncMessage.GetBlockResponse[RobotAgreement]] =
    Codec.deriveLabelledGeneric

  implicit val syncMessageCodec: Codec[SyncMessage[RobotAgreement]] =
    discriminated[SyncMessage[RobotAgreement]]
      .by(uint4)
      .typecase(0, getStatusRequestCodec)
      .typecase(1, getStatusResponseCodec)
      .typecase(2, getBlockRequestCodec)
      .typecase(3, getBlockResponseCodec)

  implicit val hotstuffConsensusMessageCodec
      : Codec[HotStuffMessage.ConsensusMessage[RobotAgreement]] =
    Codec.deriveLabelledGeneric

  implicit val hotstuffSyncMessageCodec
      : Codec[HotStuffMessage.SyncMessage[RobotAgreement]] =
    Codec.deriveLabelledGeneric

  implicit val hotstuffMessageCodec: Codec[HotStuffMessage[RobotAgreement]] =
    discriminated[HotStuffMessage[RobotAgreement]]
      .by(uint4)
      .typecase(0, hotstuffConsensusMessageCodec)
      .typecase(1, hotstuffSyncMessageCodec)

  implicit val agreementMessageCodec
      : Codec[DuplexMessage.AgreementMessage[RobotAgreement]] =
    Codec.deriveLabelledGeneric

  implicit val applicationMessageCodec
      : Codec[DuplexMessage.ApplicationMessage[RobotMessage]] =
    Codec.deriveLabelledGeneric

  implicit val duplexMessageCodec
      : Codec[DuplexMessage[RobotAgreement, RobotMessage]] =
    discriminated[DuplexMessage[RobotAgreement, RobotMessage]]
      .by(uint4)
      .typecase(0, agreementMessageCodec)
      .typecase(1, applicationMessageCodec)
}
