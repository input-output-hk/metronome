package io.iohk.metronome.checkpointing.app.config

import com.typesafe.config.Config
import io.iohk.metronome.config.{ConfigParser, ConfigDecoders}
import io.iohk.metronome.crypto.{ECPublicKey, ECPrivateKey}
import io.circe._, io.circe.generic.semiauto._
import java.nio.file.Path
import scodec.bits.ByteVector
import scala.util.Try

object CheckpointingConfigParser {
  def parse(root: Config): ConfigParser.Result[CheckpointingConfig] = {
    ConfigParser.parse[CheckpointingConfig](
      root.getConfig("metronome.checkpointing").root(),
      prefix = "METRONOME_CHECKPOINTING"
    )
  }

  import ConfigDecoders._

  def hexDecoder[T](f: ByteVector => T): Decoder[T] =
    Decoder[String].emap { str =>
      ByteVector.fromHex(str) match {
        case None =>
          Left("$str is not a valid hexadecimal value")
        case Some(bytes) =>
          Try(f(bytes)).toEither.left.map(_.getMessage)
      }
    }

  implicit val ecPublicKeyDecoder: Decoder[ECPublicKey] =
    hexDecoder(ECPublicKey(_))

  implicit val ecPrivateKeyDecoder: Decoder[ECPrivateKey] =
    hexDecoder(ECPrivateKey(_))

  implicit val nodeDecoder: Decoder[CheckpointingConfig.Node] =
    deriveDecoder

  implicit val federationDecoder: Decoder[CheckpointingConfig.Federation] =
    deriveDecoder

  implicit val networkDecoder: Decoder[CheckpointingConfig.Network] =
    deriveDecoder

  implicit val pathDecoder: Decoder[Path] =
    Decoder[String].map(Path.of(_))

  implicit val dbDecoder: Decoder[CheckpointingConfig.Database] =
    deriveDecoder

  implicit val consensusDecoder: Decoder[CheckpointingConfig.Consensus] =
    deriveDecoder

  implicit val configDecoder: Decoder[CheckpointingConfig] = deriveDecoder
}
