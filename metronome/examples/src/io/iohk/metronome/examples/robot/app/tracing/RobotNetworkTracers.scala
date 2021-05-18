package io.iohk.metronome.examples.robot.app.tracing

import monix.eval.Task
import io.iohk.metronome.examples.robot.RobotAgreement
import io.iohk.metronome.examples.robot.service.messages.RobotMessage
import io.iohk.metronome.hotstuff.service.messages.DuplexMessage
import io.iohk.metronome.networking.{NetworkTracers, NetworkEvent}
import io.iohk.metronome.tracer.Tracer
import io.iohk.metronome.logging.{HybridLog, HybridLogObject, LogTracer}
import io.circe.{Encoder, JsonObject}

object RobotNetworkTracers {
  type RobotNetworkMessage = DuplexMessage[RobotAgreement, RobotMessage]
  type RobotNetworkEvent =
    NetworkEvent[RobotAgreement.PKey, RobotNetworkMessage]

  implicit val networkEventHybridLog: HybridLog[RobotNetworkEvent] = {
    import NetworkEvent._
    import io.circe.syntax._

    implicit val keyEncoder: Encoder[RobotAgreement.PKey] =
      Encoder[String].contramap[RobotAgreement.PKey](_.bytes.toHex)

    implicit val peerEncoder: Encoder.AsObject[Peer[RobotAgreement.PKey]] =
      Encoder.AsObject.instance { case Peer(key, address) =>
        JsonObject(
          "publicKey" -> key.asJson,
          "address"   -> address.toString.asJson
        )
      }

    HybridLog.instance[RobotNetworkEvent](
      level = _ => HybridLogObject.Level.Debug,
      message = _.getClass.getSimpleName,
      event = {
        case e: ConnectionUnknown[_]      => e.peer.asJsonObject
        case e: ConnectionRegistered[_]   => e.peer.asJsonObject
        case e: ConnectionDeregistered[_] => e.peer.asJsonObject
        case e: ConnectionDiscarded[_]    => e.peer.asJsonObject
        case e: ConnectionSendError[_]    => e.peer.asJsonObject
        case e: ConnectionFailed[_] =>
          e.peer.asJsonObject.add("error", e.error.toString.asJson)
        case e: ConnectionReceiveError[_] =>
          e.peer.asJsonObject.add("error", e.error.toString.asJson)
        case e: NetworkEvent.MessageReceived[_, _] =>
          e.peer.asJsonObject
            .add("messageType", e.message.getClass.getSimpleName.asJson)
        case e: NetworkEvent.MessageSent[_, _] =>
          e.peer.asJsonObject
            .add("messageType", e.message.getClass.getSimpleName.asJson)
      }
    )
  }

  implicit val networkEventTracer: Tracer[Task, RobotNetworkEvent] =
    LogTracer.hybrid[Task, RobotNetworkEvent]

  implicit val networkTracers
      : NetworkTracers[Task, RobotAgreement.PKey, RobotNetworkMessage] =
    NetworkTracers(networkEventTracer)
}
