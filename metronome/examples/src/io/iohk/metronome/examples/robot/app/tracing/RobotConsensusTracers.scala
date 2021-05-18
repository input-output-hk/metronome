package io.iohk.metronome.examples.robot.app.tracing

import monix.eval.Task
import io.iohk.metronome.examples.robot.RobotAgreement
import io.iohk.metronome.hotstuff.service.tracing.{
  ConsensusEvent,
  ConsensusTracers
}
import io.iohk.metronome.tracer.Tracer
import io.iohk.metronome.logging.{HybridLog, HybridLogObject, LogTracer}
import io.circe.{Encoder, JsonObject, Json}
import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.crypto.ECPublicKey
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.VotingPhase

object RobotConsensusTracers {

  type RobotConsensusEvent = ConsensusEvent[RobotAgreement]

  implicit val consensusEventHybridLog
      : HybridLog[ConsensusEvent[RobotAgreement]] = {
    import ConsensusEvent._
    import io.circe.syntax._

    implicit val viewNumberEncoder: Encoder[ViewNumber] =
      Encoder[Long].contramap[ViewNumber](identity)

    implicit val hashEncoder: Encoder[Hash] =
      Encoder[String].contramap[Hash](_.toHex)

    implicit val phaseEncoder: Encoder[VotingPhase] =
      Encoder[String].contramap[VotingPhase](_.toString)

    implicit val publicKeyEncoder: Encoder[ECPublicKey] =
      Encoder[String].contramap[ECPublicKey](_.bytes.toHex)

    HybridLog.instance[RobotConsensusEvent](
      level = {
        case e: Error if e.message.contains("SendMessage") =>
          HybridLogObject.Level.Debug
        case _: Error        => HybridLogObject.Level.Error
        case _: Timeout      => HybridLogObject.Level.Warn
        case _: Rejected[_]  => HybridLogObject.Level.Warn
        case _: ViewSync     => HybridLogObject.Level.Info
        case _: AdoptView[_] => HybridLogObject.Level.Info
        case _               => HybridLogObject.Level.Debug
      },
      message = _.getClass.getSimpleName,
      event = {
        case e: Timeout =>
          JsonObject(
            "viewNumber" -> e.viewNumber.asJson,
            "messageCounter" -> Json.obj(
              "past"    -> e.messageCounter.past.asJson,
              "present" -> e.messageCounter.present.asJson,
              "future"  -> e.messageCounter.future.asJson
            )
          )

        case e: ViewSync =>
          JsonObject("viewNumber" -> e.viewNumber.asJson)

        case e: AdoptView[_] =>
          JsonObject(
            "viewNumber" -> e.status.viewNumber.asJson,
            "blockHash"  -> e.status.commitQC.blockHash.asJson
          )

        case e: NewView =>
          JsonObject("viewNumber" -> e.viewNumber.asJson)

        case e: Quorum[_] =>
          JsonObject(
            "viewNumber" -> e.quorumCertificate.viewNumber.asJson,
            "phase"      -> e.quorumCertificate.phase.asJson,
            "blockHash"  -> e.quorumCertificate.blockHash.asJson
          )

        case e: FromPast[_] =>
          JsonObject(
            "viewNumber"  -> e.message.message.viewNumber.asJson,
            "messageType" -> e.message.message.getClass.getSimpleName.asJson,
            "sender"      -> e.message.sender.asJson
          )

        case e: FromFuture[_] =>
          JsonObject(
            "viewNumber"  -> e.message.message.viewNumber.asJson,
            "messageType" -> e.message.message.getClass.getSimpleName.asJson,
            "sender"      -> e.message.sender.asJson
          )

        case e: Stashed[_] =>
          JsonObject(
            "viewNumber"  -> e.error.event.message.viewNumber.asJson,
            "messageType" -> e.error.event.message.getClass.getSimpleName.asJson,
            "sender"      -> e.error.event.sender.asJson
          )

        case e: Rejected[_] =>
          JsonObject(
            "errorType" -> e.error.getClass.getSimpleName.asJson,
            "error"     -> e.error.toString.asJson
          )

        case e: ExecutionSkipped[_] =>
          JsonObject(
            "blockHash" -> e.blockHash.asJson
          )

        case e: BlockExecuted[_] =>
          JsonObject(
            "blockHash" -> e.blockHash.asJson
          )

        case e: Error =>
          JsonObject(
            "message" -> e.message.asJson,
            "error"   -> e.error.getMessage.asJson
          )
      }
    )
  }

  implicit val consensusEventTracer: Tracer[Task, RobotConsensusEvent] =
    LogTracer.hybrid[Task, RobotConsensusEvent]

  implicit val consensusTracers: ConsensusTracers[Task, RobotAgreement] =
    ConsensusTracers(consensusEventTracer)
}
