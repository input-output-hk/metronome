package io.iohk.metronome.checkpointing.app.config

import com.typesafe.config.ConfigFactory
import io.iohk.metronome.config.{ConfigParser, ConfigDecoders}
import io.circe._, io.circe.generic.semiauto._

object CheckpointingConfigParser {
  def parse: ConfigParser.Result[CheckpointingConfig] = {
    ConfigParser.parse[CheckpointingConfig](
      ConfigFactory.load().getConfig("metronome.checkpointing").root(),
      prefix = "METRONOME_CHECKPOINTING"
    )
  }

  implicit val configDecoder: Decoder[CheckpointingConfig] = deriveDecoder
}
