package io.iohk.metronome.config

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class ConfigParserSpec
    extends AnyFlatSpec
    with Matchers
    with TableDrivenPropertyChecks {
  "toJson" should "parse simple.conf to JSON" in {
    val conf = ConfigFactory.load("simple.conf")
    val json = ConfigParser.toJson(conf.getConfig("simple").root())
    json.noSpaces shouldBe """{"nested-structure":{"bar_baz":{"spam":"eggs"},"foo":10}}"""
  }

  "toCamelCase" should "turn keys into camel case" in {
    val examples = Table(
      ("input", "expected"),
      ("nested-structure", "nestedStructure"),
      ("nested_structure", "nestedStructure"),
      ("multiple-dashes_and_underscores", "multipleDashesAndUnderscores"),
      ("multiple-dashes_and_underscores", "multipleDashesAndUnderscores"),
      ("camelCasedKeys", "camelCasedKeys")
    )
    forAll(examples) { case (input, expected) =>
      ConfigParser.toCamelCase(input) shouldBe expected
    }
  }

  it should "turn all keys in a JSON object into camel case" in {
    val conf = ConfigFactory.load("simple.conf")
    val orig = ConfigParser.toJson(conf.root())
    val json = (ConfigParser.toCamelCase(orig) \\ "simple").head
    json.noSpaces shouldBe """{"nestedStructure":{"barBaz":{"spam":"eggs"},"foo":10}}"""
  }
}
