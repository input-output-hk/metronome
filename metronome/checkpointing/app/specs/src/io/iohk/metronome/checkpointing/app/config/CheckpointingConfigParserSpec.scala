package io.iohk.metronome.checkpointing.app.config

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers

class CheckpointingConfigParserSpec
    extends AnyFlatSpec
    with Inside
    with Matchers {

  behavior of "CheckpointingConfigParser"

  it should "not parse the default configuration due to missing data" in {
    inside(CheckpointingConfigParser.parse(ConfigFactory.load())) {
      case Left(_) =>
        succeed
    }
  }

  it should "parse the with the test overrides" in {
    inside(
      CheckpointingConfigParser.parse(ConfigFactory.load("test.conf"))
    ) { case Right(config) =>
      config.remote.listen.port shouldBe config.federation.self.port
    }
  }
}
