package io.iohk.metronome.config

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.Inside

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

}
