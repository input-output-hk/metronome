package io.iohk.metronome.examples.robot.app.config

import scopt.OParser

/** Command Line Options. */
case class RobotOptions(
    nodeIndex: Int = 0
)

object RobotOptions {

  /** Parse the options. Return `None` if there was an error,
    * which has already been printed to the console.
    */
  def parse(config: RobotConfig, args: List[String]): Option[RobotOptions] =
    OParser.parse(
      RobotOptions.oparser(config),
      args,
      RobotOptions()
    )

  private def oparser(config: RobotConfig) = {
    val builder = OParser.builder[RobotOptions]
    import builder._

    OParser.sequence(
      programName("robot"),
      opt[Int]('i', "node-index")
        .action((i, opts) => opts.copy(nodeIndex = i))
        .text("index of example node to run")
        .required()
        .validate(i =>
          Either.cond(
            0 <= i && i < config.network.nodes.length,
            (),
            s"Must be between 0 and ${config.network.nodes.length - 1}"
          )
        )
    )
  }
}
