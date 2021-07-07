package io.iohk.metronome.examples.robot.app.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.Inside

class RobotConfigParserSpec extends AnyFlatSpec with Inside {
  behavior of "RobotConfigParser"

  it should "parse the default configuration" in {
    inside(RobotConfigParser.parse) { case Right(_) =>
      succeed
    }
  }
}
