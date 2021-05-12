package io.iohk.metronome.examples.robot.app.config

import com.typesafe.config.ConfigFactory
import io.iohk.metronome.config.ConfigParser
import io.circe._, io.circe.generic.semiauto._
import io.iohk.metronome.crypto.{ECPublicKey, ECPrivateKey}
import scodec.bits.ByteVector
import scala.util.Try

object RobotConfigParser {

  def parse: ConfigParser.Result[RobotConfig] =
    ConfigParser.parse[RobotConfig](
      ConfigFactory.load().getConfig("metronome.examples.robot").root(),
      prefix = "METRONOME_ROBOT"
    )

  implicit val ecPublicKeyDecoder: Decoder[ECPublicKey] =
    implicitly[Decoder[String]].emap { str =>
      ByteVector.fromHex(str) match {
        case None =>
          Left("$str is not a valid hexadecimal key")
        case Some(bytes) =>
          Try(ECPublicKey(bytes)).toEither.left.map(_.getMessage)
      }
    }

  implicit val ecPrivateKeyDecoder: Decoder[ECPrivateKey] =
    implicitly[Decoder[String]].emap { str =>
      ByteVector.fromHex(str) match {
        case None =>
          Left("$str is not a valid hexadecimal key")
        case Some(bytes) =>
          Try(ECPrivateKey(bytes)).toEither.left.map(_.getMessage)
      }
    }

  implicit val nodeDecoder: Decoder[RobotConfig.Node] = deriveDecoder
  implicit val configDecoder: Decoder[RobotConfig]    = deriveDecoder

}
