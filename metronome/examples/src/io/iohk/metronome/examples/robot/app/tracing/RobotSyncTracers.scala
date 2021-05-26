package io.iohk.metronome.examples.robot.app.tracing

import monix.eval.Task
import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.crypto.ECPublicKey
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.service.tracing.{SyncEvent, SyncTracers}
import io.iohk.metronome.logging.{HybridLog, HybridLogObject, LogTracer}
import io.iohk.metronome.examples.robot.RobotAgreement
import io.circe.{Encoder, JsonObject, Json}

object RobotSyncTracers {

  type RobotSyncEvent = SyncEvent[RobotAgreement]

  implicit val syncEventHybridLog: HybridLog[SyncEvent[RobotAgreement]] = {
    import SyncEvent._
    import io.circe.syntax._

    implicit val viewNumberEncoder: Encoder[ViewNumber] =
      Encoder[Long].contramap[ViewNumber](identity)

    implicit val hashEncoder: Encoder[Hash] =
      Encoder[String].contramap[Hash](_.toHex)

    implicit val publicKeyEncoder: Encoder[ECPublicKey] =
      Encoder[String].contramap[ECPublicKey](_.bytes.toHex)

    HybridLog.instance[RobotSyncEvent](
      level = {
        case _: Error              => HybridLogObject.Level.Error
        case _: InvalidStatus[_]   => HybridLogObject.Level.Warn
        case _: RequestTimeout[_]  => HybridLogObject.Level.Warn
        case _: ResponseIgnored[_] => HybridLogObject.Level.Warn
        case _: QueueFull[_]       => HybridLogObject.Level.Warn
        case _: StatusPoll[_]      => HybridLogObject.Level.Info
        case _                     => HybridLogObject.Level.Debug
      },
      message = _.getClass.getSimpleName,
      event = {
        case e: QueueFull[_] =>
          JsonObject("sender" -> e.sender.asJson)

        case e: RequestTimeout[_] =>
          JsonObject(
            "recipient"   -> e.recipient.asJson,
            "requestType" -> e.request.getClass.getSimpleName.asJson
          )

        case e: ResponseIgnored[_] =>
          JsonObject(
            "sender"       -> e.sender.asJson,
            "responseType" -> e.response.getClass.getSimpleName.asJson
          )

        case e: StatusPoll[_] =>
          JsonObject(
            "statuses" -> Json.arr(
              e.statuses.toSeq.map { case (publicKey, status) =>
                Json.obj(
                  "publicKey"  -> publicKey.asJson,
                  "viewNumber" -> status.viewNumber.asJson,
                  "commitQC" -> Json.obj(
                    "viewNumber" -> status.commitQC.viewNumber.asJson,
                    "blockHash"  -> status.commitQC.blockHash.asJson
                  )
                )
              }: _*
            )
          )

        case e: InvalidStatus[_] =>
          JsonObject(
            "sender" -> e.error.sender.asJson,
            "hint"   -> e.hint.asJson
          )

        case e: Error =>
          JsonObject(
            "error" -> e.error.getMessage.asJson
          )
      }
    )
  }

  implicit val syncEventHybridLogTracer =
    LogTracer.hybrid[Task, RobotSyncEvent]

  implicit val syncHybridLogTracers =
    SyncTracers(syncEventHybridLogTracer)
}
