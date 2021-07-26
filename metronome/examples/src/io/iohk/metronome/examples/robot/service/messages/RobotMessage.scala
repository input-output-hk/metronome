package io.iohk.metronome.examples.robot.service.messages

import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.core.messages.{RPCMessage, RPCMessageCompanion}
import io.iohk.metronome.examples.robot.models.Robot

sealed trait RobotMessage { self: RPCMessage => }

object RobotMessage extends RPCMessageCompanion {

  case class GetStateRequest(
      requestId: RequestId,
      stateHash: Hash
  ) extends RobotMessage
      with Request

  case class GetStateResponse(
      requestId: RequestId,
      state: Robot.State
  ) extends RobotMessage
      with Response

  implicit val getStatePair =
    pair[GetStateRequest, GetStateResponse]
}
