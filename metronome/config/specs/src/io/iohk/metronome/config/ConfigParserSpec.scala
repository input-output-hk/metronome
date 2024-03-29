package io.iohk.metronome.config

import com.typesafe.config.ConfigFactory
import io.circe.Decoder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.Inside
import scala.concurrent.duration._
import com.typesafe.config.Config
import org.scalatest.compatible.Assertion

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

  private def vectorFallback[T: Decoder](
      unwrapper: T => Vector[String]
  ): Unit = {

    it should "handle JSON Object with sequential indices successfully" in {
      val withArray = """{"field":["valueA", "valueB", "valueC"]}"""
      val withObject =
        """{"field":{"2":"valueC", "0": "valueA", "1":"valueB"}}"""
      val expected = Vector("valueA", "valueB", "valueC")
      val parsedArray =
        ConfigParser.parse[T](ConfigFactory.parseString(withArray).root)
      val parsedObject =
        ConfigParser.parse[T](ConfigFactory.parseString(withObject).root)
      inside(parsedArray) { case Right(value) =>
        unwrapper(value) shouldBe expected
      }
      inside(parsedObject) { case Right(value) =>
        unwrapper(value) shouldBe expected
      }
    }

    it should "fail on JSON Object key gaps" in {
      val gapsConf1 =
        ConfigFactory.parseString("""{"field":{"2":"valueC", "0":"valueA"}}""")
      val gapsConf2 = ConfigFactory.parseString("""{"field":{"2":"valueA"}}""")
      checkDecoding[T](
        gapsConf1,
        Left(s"Expected [0, 2) sequence, but got {0, 2}")
      )
      checkDecoding[T](
        gapsConf2,
        Left(s"Expected [0, 1) sequence, but got {2}")
      )
    }

    it should "succeed on empty JSON Object" in {
      val emptyConf = ConfigFactory.parseString("""{"field":{}}""")
      checkDecoding[T](
        emptyConf,
        Right(outer => unwrapper(outer) shouldBe empty)
      )
    }

    it should "succeed on JSON Object with duplicated keys keeping the last value" in {
      val src =
        """|{
           |  "field":{
           |    "0": "valueA", 
           |    "1": "valueB", 
           |    "0": "valueC", 
           |    "1": "valueD", 
           |    "0": "valueE", 
           |    "1": "valueF"
           |  }
           |}""".stripMargin
      val dupesConf = ConfigFactory.parseString(src)
      val expected  = Vector("valueE", "valueF")
      checkDecoding[T](
        dupesConf,
        Right(outer => unwrapper(outer) shouldBe expected)
      )
    }

    it should "work correctly with multiple system property override" in {
      withProperties(("field.0", "valueX"), ("field.1", "valueY")) {
        val expected = Vector("valueX", "valueY")
        val root     = ConfigFactory.load("array.conf").root
        val config   = ConfigParser.parse[T](root)
        inside(config) { case Right(result) =>
          unwrapper(result) shouldBe expected
        }
      }
    }

    it should "report a meaningful error text on incorrect array override" in {
      withProperties(("field.2", "valueX"), ("field.1", "valueY")) {
        val root   = ConfigFactory.load("array.conf").root
        val config = ConfigParser.parse[T](root)
        inside(config) { case Left(Right(err)) =>
          err.message shouldBe "Expected [0, 2) sequence, but got {1, 2}"
        }
      }
    }

  }

  {
    import ConfigParserSpec.TestConfigWithJsonArray._
    "Seq decoder" should behave like vectorFallback[TestConfigWithSeq](
      _.field.toVector
    )
    "List decoder" should behave like vectorFallback[TestConfigWithList](
      _.field.toVector
    )
    "Vector decoder" should behave like vectorFallback[TestConfigWithVector](
      _.field
    )
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

  it should "deal with escape characters according to rfc7159 (section-7)" in {
    val conf = ConfigFactory.parseString("""{"quotation_mark":"",
        |"reverse_solidus":"",
        |"solidus":"",
        |"backspace":"",
        |"form_feed":"",
        |"line_feed":"",
        |"carriage_return":"",
        |"tab":"",
        |"java_home":"C:\\Program Files\\java\\jdk-8\\bin"}""".stripMargin)

    val orig = ConfigParser.toJson(conf.root())
    val json = ConfigParser.withCamelCase(orig)

    val env = Map(
      "QUOTATION_MARK"  -> "\"",
      "REVERSE_SOLIDUS" -> "\\",
      "SOLIDUS"         -> "/",
      "BACKSPACE"       -> "\b",
      "FORM_FEED"       -> "\f",
      "LINE_FEED"       -> "\n",
      "CARRIAGE_RETURN" -> "\r",
      "TAB"             -> "\t",
      "JAVA_HOME"       -> "C:\\Program Files\\java\\jdk-11\\bin"
    )

    val result = ConfigParser.withEnvVarOverrides(json, "", env)

    inside(result) { case Right(json) =>
      json.noSpaces shouldBe
        """{"backspace":"\b","carriageReturn":"\r","formFeed":"\f","javaHome":"C:\\Program Files\\java\\jdk-11\\bin","lineFeed":"\n","quotationMark":"\"","reverseSolidus":"\\","solidus":"/","tab":"\t"}""".stripMargin
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

    withProperties(
      ("metronome.metrics.enabled", "true"),
      ("metronome.network.max-incoming-connections", "50")
    ) {
      val config = ConfigParser.parse[TestConfig](
        ConfigFactory.load("complex.conf").getConfig("metronome").root()
      )

      inside(config) { case Right(config) =>
        config.metrics.enabled shouldBe true
        config.network.maxIncomingConnections shouldBe 50
      }
    }
  }

  private def withProperty[T](key: String, value: String)(thunk: => T): T = {
    withProperties(key -> value)(thunk)
  }

  private def withProperties[T](props: (String, String)*)(thunk: => T): T = {
    val current = props.map { case (k, v) =>
      // it is important to clear property which wasn't set before
      // that is why we're keeping None values in that Map
      k -> Option(System.setProperty(k, v))
    }.toMap
    try {
      ConfigFactory.invalidateCaches()
      thunk
    } finally {
      current.foreach {
        case (k, Some(v)) => System.setProperty(k, v)
        case (k, _)       => System.clearProperty(k)
      }
      ConfigFactory.invalidateCaches()
    }
  }

  private def checkDecoding[T: Decoder](
      source: Config,
      checker: Either[String, T => Assertion]
  ) = {
    val result = ConfigParser.parse[T](source.root)
    checker match {
      case Left(errorMessage) =>
        inside(result) { case Left(Right(err)) =>
          err.message shouldBe errorMessage
        }
      case Right(checkFn) =>
        inside(result) { case Right(value) => checkFn(value) }
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
    implicit val configWithSeqDecoder: Decoder[TestConfigWithSeq] =
      deriveDecoder
    implicit val configWithListDecoder: Decoder[TestConfigWithList] =
      deriveDecoder
    implicit val configWithVectorDecoder: Decoder[TestConfigWithVector] =
      deriveDecoder
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
