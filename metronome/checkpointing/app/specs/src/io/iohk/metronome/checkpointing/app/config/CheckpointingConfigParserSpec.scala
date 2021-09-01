package io.iohk.metronome.checkpointing.app.config

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import java.nio.file.Path

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
      config.federation.self.privateKey.isLeft shouldBe true
    }
  }

  it should "parse when the private key is a path" in {
    inside(
      CheckpointingConfigParser.parse {
        ConfigFactory.parseString(
          "metronome.checkpointing.federation.self.private-key=./node.key"
        ) withFallback ConfigFactory.load("test.conf")
      }
    ) { case Right(config) =>
      config.federation.self.privateKey shouldBe Right(Path.of("./node.key"))
    }
  }
}
