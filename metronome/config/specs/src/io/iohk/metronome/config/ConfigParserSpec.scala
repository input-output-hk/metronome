package io.iohk.metronome.config

import com.typesafe.config.ConfigFactory
import io.circe.Decoder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.Inside
import scala.concurrent.duration._

class ConfigParserSpec
    extends AnyFlatSpec
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

  "toJson" should "parse simple.conf to JSON" in {
    val conf = ConfigFactory.load("simple.conf")
    val json = ConfigParser.toJson(conf.getConfig("simple").root())
    json.noSpaces shouldBe """{"nested-structure":{"bar_baz":{"spam":"eggs"},"foo":10}}"""
  }

  "toCamelCase" should "turn keys into camelCase" in {
    val examples = Table(
      ("input", "expected"),
      ("nested-structure", "nestedStructure"),
      ("nested_structure", "nestedStructure"),
      ("multiple-dashes_and_underscores", "multipleDashesAndUnderscores"),
      ("multiple-dashes_and_underscores", "multipleDashesAndUnderscores"),
      ("camelCaseKey", "camelCaseKey")
    )
    forAll(examples) { case (input, expected) =>
      ConfigParser.toCamelCase(input) shouldBe expected
    }
  }

  "toSnakeCase" should "turn camelCase keys into SNAKE_CASE" in {
    val examples = Table(
      ("input", "expected"),
      ("nestedStructure", "NESTED_STRUCTURE"),
      ("nested_structure", "NESTED_STRUCTURE"),
      ("nested-structure", "NESTED_STRUCTURE")
    )
    forAll(examples) { case (input, expected) =>
      ConfigParser.toSnakeCase(input) shouldBe expected
    }
  }

  "withCamelCase" should "turn all keys in a JSON object into camelCase" in {
    val conf = ConfigFactory.load("simple.conf")
    val orig = ConfigParser.toJson(conf.root())
    val json = (ConfigParser.withCamelCase(orig) \\ "simple").head
    json.noSpaces shouldBe """{"nestedStructure":{"barBaz":{"spam":"eggs"},"foo":10}}"""
  }

  "withEnvVarOverrides" should "overwrite keys from the environment" in {
    val conf = ConfigFactory.load("override.conf")
    val orig = ConfigParser.toJson(conf.getConfig("override").root())
    val json = ConfigParser.withCamelCase(orig)

    val env = Map(
      "TEST_METRICS_ENABLED"     -> "true",
      "TEST_NETWORK_BOOTSTRAP_0" -> "localhost:50000",
      "TEST_OPTIONAL"            -> "test",
      "TEST_NUMERIC"             -> "456",
      "TEST_TEXTUAL"             -> "Terra Nostra",
      "TEST_BOOLEAN"             -> "false"
    )

    val result = ConfigParser.withEnvVarOverrides(json, "TEST", env)

    inside(result) { case Right(json) =>
      json.noSpaces shouldBe """{"boolean":false,"metrics":{"enabled":true},"network":{"bootstrap":["localhost:50000","localhost:40002"]},"numeric":456,"optional":"test","textual":"Terra Nostra"}"""
    }
  }

  it should "validate that data types are not altered" in {
    val conf = ConfigFactory.load("override.conf")
    val orig = ConfigParser.toJson(conf.root())
    val json = ConfigParser.withCamelCase(orig)

    val examples = Table(
      ("path", "invalid"),
      ("OVERRIDE_NUMERIC", "NaN"),
      ("OVERRIDE_TEXTUAL", "123"),
      ("OVERRIDE_BOOLEAN", "no")
    )
    forAll(examples) { case (path, invalid) =>
      ConfigParser
        .withEnvVarOverrides(json, "", Map(path -> invalid))
        .isLeft shouldBe true
    }
  }

  "parse" should "decode into a configuration model" in {
    import ConfigParserSpec.TestConfig

    val config = ConfigParser.parse[TestConfig](
      ConfigFactory.load("complex.conf").getConfig("metronome").root(),
      prefix = "TEST",
      env = Map("TEST_METRICS_ENABLED" -> "true")
    )

    inside(config) { case Right(config) =>
      config shouldBe TestConfig(
        TestConfig.Metrics(enabled = true),
        TestConfig.Network(
          bootstrap = List("localhost:40001"),
          timeout = 5.seconds,
          maxPacketSize = TestConfig.Size(512000),
          maxIncomingConnections = 10,
          clientId = None
        ),
        TestConfig
          .Blockchain(
            maxBlockSize = TestConfig.Size(10000000),
            viewTimeout = 15.seconds
          ),
        chainId = Some("test-chain")
      )
    }
  }

  it should "work with system property overrides" in {
    import ConfigParserSpec.TestConfig

    withProperty("metronome.metrics.enabled", "true") {
      withProperty("metronome.network.max-incoming-connections", "50") {
        val config = ConfigParser.parse[TestConfig](
          ConfigFactory.load("complex.conf").getConfig("metronome").root()
        )

        inside(config) { case Right(config) =>
          config.metrics.enabled shouldBe true
          config.network.maxIncomingConnections shouldBe 50
        }
      }
    }
  }

  def withProperty[T](key: String, value: String)(thunk: => T): T = {
    val maybeCurrent = Option(System.setProperty(key, value))
    try {
      ConfigFactory.invalidateCaches()
      thunk
    } finally {
      maybeCurrent.foreach(System.setProperty(key, _))
      ConfigFactory.invalidateCaches()
    }
  }
}

object ConfigParserSpec {
  import io.circe._, io.circe.generic.semiauto._

  case class TestConfig(
      metrics: TestConfig.Metrics,
      network: TestConfig.Network,
      blockchain: TestConfig.Blockchain,
      chainId: Option[String]
  )
  object TestConfig {
    import ConfigDecoders.{durationDecoder, booleanDecoder}

    case class Metrics(enabled: Boolean)
    object Metrics {
      implicit val decoder: Decoder[Metrics] =
        deriveDecoder
    }

    case class Network(
        bootstrap: List[String],
        timeout: FiniteDuration,
        maxPacketSize: Size,
        maxIncomingConnections: Int,
        clientId: Option[String]
    )
    object Network {
      implicit val decoder: Decoder[Network] =
        deriveDecoder
    }

    case class Size(bytes: Long)
    object Size {
      implicit val decoder: Decoder[Size] =
        ConfigDecoders.bytesDecoder.map(Size(_))
    }

    case class Blockchain(
        maxBlockSize: Size,
        viewTimeout: FiniteDuration
    )
    object Blockchain {
      implicit val decoder: Decoder[Blockchain] =
        ConfigDecoders.strategyDecoder[Blockchain]("consensus", deriveDecoder)
    }

    implicit val decoder: Decoder[TestConfig] =
      deriveDecoder
  }
}
