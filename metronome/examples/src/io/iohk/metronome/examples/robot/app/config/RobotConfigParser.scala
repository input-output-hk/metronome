package io.iohk.metronome.examples.robot.app.config

import com.typesafe.config.ConfigFactory
import io.iohk.metronome.config.{ConfigParser, ConfigDecoders}
import io.circe._, io.circe.generic.semiauto._
import io.iohk.metronome.crypto.{ECPublicKey, ECPrivateKey}
import java.nio.file.Path
import scodec.bits.ByteVector
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object RobotConfigParser {

  def parse: ConfigParser.Result[RobotConfig] = {
    ConfigParser.parse[RobotConfig](
      ConfigFactory.load().getConfig("metronome.examples.robot").root(),
      prefix = "METRONOME_ROBOT"
    )
  }

  implicit val ecPublicKeyDecoder: Decoder[ECPublicKey] =
    Decoder[String].emap { str =>
      ByteVector.fromHex(str) match {
        case None =>
          Left("$str is not a valid hexadecimal key")
        case Some(bytes) =>
          Try(ECPublicKey(bytes)).toEither.left.map(_.getMessage)
      }
    }

  implicit val ecPrivateKeyDecoder: Decoder[ECPrivateKey] =
    Decoder[String].emap { str =>
      ByteVector.fromHex(str) match {
        case None =>
          Left("$str is not a valid hexadecimal key")
        case Some(bytes) =>
          Try(ECPrivateKey(bytes)).toEither.left.map(_.getMessage)
      }
    }

  implicit val pathDecoder: Decoder[Path] =
    Decoder[String].map(Path.of(_))

  implicit val finiteDurationDecoder: Decoder[FiniteDuration] =
    ConfigDecoders.durationDecoder

  implicit val nodeDecoder: Decoder[RobotConfig.Node]         = deriveDecoder
  implicit val networkDecoder: Decoder[RobotConfig.Network]   = deriveDecoder
  implicit val databaseDecoder: Decoder[RobotConfig.Database] = deriveDecoder
  implicit val modelDecoder: Decoder[RobotConfig.Model]       = deriveDecoder
  implicit val configDecoder: Decoder[RobotConfig]            = deriveDecoder

}
