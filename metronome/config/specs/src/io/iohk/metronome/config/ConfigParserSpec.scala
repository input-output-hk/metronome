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

  "seq,list,vector decoders" should "handle JSON Object with sequential indices successfully" in {
    import ConfigParserSpec.TestConfigWithJsonArray._
    val withArray = """{"field":["valueA", "valueB", "valueC"]}"""
    val withObject = """{"field":{"2":"valueC", "0": "valueA", "1":"valueB"}}"""
    val expected = Vector("valueA", "valueB", "valueC")
    val aConf = ConfigFactory.parseString(withArray)
    val oConf = ConfigFactory.parseString(withObject)

    // seqs
    val aSeq = ConfigParser.parse[TestConfigWithSeq](aConf.root)
    val oSeq = ConfigParser.parse[TestConfigWithSeq](oConf.root)
    inside(aSeq) { case Right(value) => value.field.toVector shouldBe expected }
    aSeq shouldEqual oSeq

    // lists
    val aList = ConfigParser.parse[TestConfigWithList](aConf.root)
    val oList = ConfigParser.parse[TestConfigWithList](oConf.root)
    inside(aList) { case Right(value) => value.field.toVector shouldBe expected }
    aList shouldEqual oList

    // vectors
    val aVector = ConfigParser.parse[TestConfigWithVector](aConf.root)
    val oVector = ConfigParser.parse[TestConfigWithVector](oConf.root)
    inside(aVector) { case Right(value) => value.field shouldBe expected }
    aVector shouldEqual oVector
  }

  "seq,list,vector decoders" should "fail on JSON Object key gaps but allow empty object" in {
    import ConfigParserSpec.TestConfigWithJsonArray._

    val withGaps1 = """{"field":{"2":"valueC", "0":"valueA"}}"""
    val withGaps2 = """{"field":{"2":"valueA"}}"""
    val withDupes = """{"field":{"2":"valueA", "2":"valueB"}}"""
    val withEmpty = """{"field":{}}"""
    val gapsConf1 = ConfigFactory.parseString(withGaps1)
    val gapsConf2 = ConfigFactory.parseString(withGaps2)
    val dupesConf = ConfigFactory.parseString(withDupes)
    val emptyConf = ConfigFactory.parseString(withEmpty)

    // seqs
    val gapsSeq1 = ConfigParser.parse[TestConfigWithSeq](gapsConf1.root)
    val gapsSeq2 = ConfigParser.parse[TestConfigWithSeq](gapsConf2.root)
    val dupesSeq = ConfigParser.parse[TestConfigWithSeq](dupesConf.root)
    val emptySeq = ConfigParser.parse[TestConfigWithSeq](emptyConf.root)

    inside(gapsSeq1) { case Left(Right(err)) => err.message shouldBe (s"Expected [0, 2) sequence, but got {0, 2}") }
    inside(gapsSeq2) { case Left(Right(err)) => err.message shouldBe (s"Expected [0, 1) sequence, but got {2}") }
    inside(dupesSeq) { case Left(Right(err)) => err.message shouldBe (s"Expected [0, 1) sequence, but got {2}") }
    inside(emptySeq) { case Right(success) => success.field shouldBe empty }

    // lists
    val gapsList1 = ConfigParser.parse[TestConfigWithList](gapsConf1.root)
    val gapsList2 = ConfigParser.parse[TestConfigWithList](gapsConf2.root)
    val dupesList = ConfigParser.parse[TestConfigWithList](dupesConf.root)
    val emptyList = ConfigParser.parse[TestConfigWithList](emptyConf.root)

    inside(gapsList1) { case Left(Right(err)) => err.message shouldBe (s"Expected [0, 2) sequence, but got {0, 2}") }
    inside(gapsList2) { case Left(Right(err)) => err.message shouldBe (s"Expected [0, 1) sequence, but got {2}") }
    inside(dupesList) { case Left(Right(err)) => err.message shouldBe (s"Expected [0, 1) sequence, but got {2}") }
    inside(emptyList) { case Right(success) => success.field shouldBe empty }

    // vectors
    val gapsVector1 = ConfigParser.parse[TestConfigWithVector](gapsConf1.root)
    val gapsVector2 = ConfigParser.parse[TestConfigWithVector](gapsConf2.root)
    val dupesVector = ConfigParser.parse[TestConfigWithVector](dupesConf.root)
    val emptyVector = ConfigParser.parse[TestConfigWithVector](emptyConf.root)

    inside(gapsVector1) { case Left(Right(err)) => err.message shouldBe (s"Expected [0, 2) sequence, but got {0, 2}") }
    inside(gapsVector2) { case Left(Right(err)) => err.message shouldBe (s"Expected [0, 1) sequence, but got {2}") }
    inside(dupesVector) { case Left(Right(err)) => err.message shouldBe (s"Expected [0, 1) sequence, but got {2}") }
    inside(emptyVector) { case Right(success) => success.field shouldBe empty }
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

  object TestConfigWithJsonArray {
    import ConfigDecoders._
    case class TestConfigWithSeq(field: Seq[String])
    case class TestConfigWithList(field: List[String])
    case class TestConfigWithVector(field: Vector[String])
    implicit val configWithSeqDecoder: Decoder[TestConfigWithSeq] = deriveDecoder
    implicit val configWithListDecoder: Decoder[TestConfigWithList] = deriveDecoder
    implicit val configWithVectorDecoder: Decoder[TestConfigWithVector] = deriveDecoder
  }

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
