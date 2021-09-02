package io.iohk.metronome.checkpointing.app.tracing

import monix.eval.Task
import io.iohk.metronome.crypto.ECPublicKey
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.service.tracing.{SyncEvent, SyncTracers}
import io.iohk.metronome.logging.{HybridLog, HybridLogObject, LogTracer}
import io.iohk.metronome.checkpointing.models.{Block, Ledger}
import io.circe.{Encoder, JsonObject, Json}
import io.iohk.metronome.checkpointing.service.tracing.CheckpointingEvent
import io.iohk.metronome.checkpointing.models.Transaction.CheckpointCandidate

object CheckpointingServiceTracers {
  import CheckpointingEvent._
  import io.circe.syntax._

  implicit val serviceEventHybridLog: HybridLog[Task, CheckpointingEvent] = {
    implicit val blockHashEncoder: Encoder[Block.Hash] =
      Encoder[String].contramap[Block.Hash](_.toHex)

    implicit val ledgerHashEncoder: Encoder[Ledger.Hash] =
      Encoder[String].contramap[Ledger.Hash](_.toHex)

    implicit val publicKeyEncoder: Encoder[ECPublicKey] =
      Encoder[String].contramap[ECPublicKey](_.bytes.toHex)

    implicit val maybeErrorEncoder: Encoder[Throwable] =
      Encoder[String].contramap[Throwable](_.getMessage)

    implicit val viewNumberEncoder: Encoder[ViewNumber] =
      Encoder[Long].contramap[ViewNumber](identity)

    HybridLog.instance[Task, CheckpointingEvent](
      level = {
        case _: Error                       => HybridLogObject.Level.Error
        case _: StateUnavailable            => HybridLogObject.Level.Error
        case _: InterpreterValidationFailed => HybridLogObject.Level.Warn
        case _: InterpreterUnavailable      => HybridLogObject.Level.Warn
        case _: InterpreterTimeout          => HybridLogObject.Level.Warn
        case _: InterpreterResponseIgnored  => HybridLogObject.Level.Warn
        case _: NewCheckpointCertificate    => HybridLogObject.Level.Info
        case _                              => HybridLogObject.Level.Debug
      },
      message = _.getClass.getSimpleName,
      event = {
        case e: InterpreterTimeout =>
          JsonObject("messageType" -> e.message.getClass.getSimpleName.asJson)
        case e: InterpreterUnavailable =>
          JsonObject("messageType" -> e.message.getClass.getSimpleName.asJson)
        case e: InterpreterResponseIgnored =>
          JsonObject(
            "messageType" -> e.message.getClass.getSimpleName.asJson,
            "error"       -> e.maybeError.asJson
          )
        case e: NetworkTimeout =>
          JsonObject(
            "messageType" -> e.message.getClass.getSimpleName.asJson,
            "recipient"   -> e.recipient.asJson
          )
        case e: NetworkResponseIgnored =>
          JsonObject(
            "messageType" -> e.message.getClass.getSimpleName.asJson,
            "from"        -> e.from.asJson,
            "error"       -> e.maybeError.asJson
          )
        case e: Proposing =>
          JsonObject(
            "blockHash" -> e.block.hash.toHex.asJson,
            "isEmpty"   -> e.block.body.transactions.isEmpty.asJson
          )
        case e: NewState =>
          JsonObject(
            "ledgerHash" -> e.state.hash.asJson
          )
        case e: NewCheckpointCertificate =>
          JsonObject(
            "viewNumber" -> e.certificate.commitQC.viewNumber.asJson
          )
        case e: StateUnavailable =>
          JsonObject(
            "blockHash"  -> e.block.hash.asJson,
            "parentHash" -> e.block.header.parentHash.asJson
          )
        case e: InterpreterValidationFailed =>
          JsonObject(
            "blockHash"  -> e.block.hash.asJson,
            "parentHash" -> e.block.header.parentHash.asJson,
            "hasCheckpoint" -> e.block.body.transactions
              .collectFirst { case CheckpointCandidate(_) => }
              .isDefined
              .asJson
          )
        case e: Error =>
          JsonObject(
            "error" -> e.error.getMessage.asJson
          )
      }
    )
  }

  implicit val serviceEventHybridLogTracer =
    LogTracer.hybrid[Task, CheckpointingEvent]
}
