package io.iohk.metronome.checkpointing.app.config

import scopt.OParser
import ch.qos.logback.classic.Level

case class CheckpointingOptions(
    nodeName: String,
    logLevel: Level
)

object CheckpointingOptions {

  val default = CheckpointingOptions(
    nodeName = "checkpointing-service",
    logLevel = Level.INFO
  )

  private val LogLevels = List(
    Level.OFF,
    Level.ERROR,
    Level.WARN,
    Level.INFO,
    Level.DEBUG,
    Level.TRACE
  )

  /** Parse the options. Return `None` if there was an error,
    * which has already been printed to the console.
    */
  def parse(
      config: CheckpointingConfig,
      args: List[String]
  ): Option[CheckpointingOptions] =
    OParser.parse(
      CheckpointingOptions.oparser(config),
      args,
      CheckpointingOptions.default
    )

  private def oparser(config: CheckpointingConfig) = {
    val builder = OParser.builder[CheckpointingOptions]
    import builder._

    OParser.sequence(
      programName("checkpointing-service"),
      opt[String]('n', "node-name")
        .action((x, opts) => opts.copy(nodeName = x))
        .text("unique name for the node")
        .required(),
      opt[String]('l', "log-level")
        .action((x, opts) => opts.copy(logLevel = Level.toLevel(x)))
        .text(
          s"log level; one of [${LogLevels.map(_.toString).mkString(" | ")}]"
        )
        .optional()
        .validate(x =>
          Either.cond(
            LogLevels.map(_.toString).contains(x.toUpperCase),
            (),
            s"Must be between one of ${LogLevels.map(_.toString)}"
          )
        )
    )
  }
}
