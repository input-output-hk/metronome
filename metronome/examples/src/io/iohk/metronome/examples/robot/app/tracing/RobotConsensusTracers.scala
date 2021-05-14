package io.iohk.metronome.examples.robot.app.tracing

import monix.eval.Task
import io.iohk.metronome.examples.robot.RobotAgreement
import io.iohk.metronome.examples.robot.service.messages.RobotMessage
import io.iohk.metronome.hotstuff.service.tracing.{
  ConsensusEvent,
  ConsensusTracers
}
import io.iohk.metronome.tracer.Tracer
import io.iohk.metronome.logging.{HybridLog, HybridLogObject, LogTracer}
import io.circe.{Encoder, JsonObject}
import io.iohk.metronome.hotstuff.consensus.ViewNumber

object RobotConsensusTracers {

  type RobotConsensusEvent = ConsensusEvent[RobotAgreement]

  implicit val consensusEventHybridLog
      : HybridLog[ConsensusEvent[RobotAgreement]] = {
    import ConsensusEvent._
    import io.circe.syntax._

    implicit val viewNumber: Encoder[ViewNumber] =
      Encoder[Long].contramap[ViewNumber](identity)

    // implicit val keyEncoder: Encoder[RobotAgreement.PKey] =
    //   Encoder[String].contramap[RobotAgreement.PKey](_.bytes.toHex)

    // implicit val peerEncoder: Encoder.AsObject[Peer[RobotAgreement.PKey]] =
    //   Encoder.AsObject.instance { case Peer(key, address) =>
    //     JsonObject("key" -> key.asJson, "address" -> address.toString.asJson)
    //   }

    HybridLog.instance[RobotConsensusEvent](
      level = _ => HybridLogObject.Level.Debug,
      message = _.getClass.getSimpleName,
      event = {
        case e: Timeout =>
          JsonObject("viewNumber" -> e.viewNumber.asJson)
        case e: ViewSync =>
          JsonObject("viewNumber" -> e.viewNumber.asJson)
        case e: AdoptView[_] =>
          JsonObject(
            "viewNumber" -> e.status.viewNumber.asJson,
            "blockHash"  -> e.status.commitQC.blockHash.asJson
          )
      }
    )
  }

  implicit val consensusEventTracer: Tracer[Task, RobotConsensusEvent] =
    LogTracer.hybrid[Task, RobotConsensusEvent]

  implicit val networkTracers
      : NetworkTracers[Task, RobotAgreement.PKey, RobotNetworkMessage] =
    NetworkTracers(networkEventTracer)
}
