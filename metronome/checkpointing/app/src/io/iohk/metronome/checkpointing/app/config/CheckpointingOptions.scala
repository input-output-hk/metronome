package io.iohk.metronome.checkpointing.app.config

import scopt.OParser
import ch.qos.logback.classic.Level

case class CheckpointingOptions(
    mode: CheckpointingOptions.Mode,
    logLevel: Level
) {
  import CheckpointingOptions.{Service, KeyGen}

  def mapService(f: Service => Service) =
    mode match {
      case m: Service => copy(mode = f(m))
      case _          => this
    }

  def logFileName: String =
    mode match {
      case m: Service => m.nodeName
      case KeyGen     => "keygen"
    }
}

object CheckpointingOptions {

  sealed trait Mode

  case class Service(
      nodeName: String
  ) extends Mode

  case object KeyGen extends Mode

  private val DefaultName = "checkpointing-service"

  private val LogLevels = List(
    Level.OFF,
    Level.ERROR,
    Level.WARN,
    Level.INFO,
    Level.DEBUG,
    Level.TRACE
  )

  val default = CheckpointingOptions(
    mode = Service(DefaultName),
    logLevel = Level.INFO
  )

  /** Parse the options. Return `None` if there was an error,
    * which has already been printed to the console.
    */
  def parse(
      args: List[String]
  ): Option[CheckpointingOptions] =
    OParser.parse(
      CheckpointingOptions.oparser,
      args,
      CheckpointingOptions.default
    )

  private val oparser = {
    val builder = OParser.builder[CheckpointingOptions]
    import builder._

    OParser.sequence(
      programName("checkpointing"),
      opt[String]('l', "log-level")
        .action((x, opts) => opts.copy(logLevel = Level.toLevel(x)))
        .text(
          s"log level; one of [${LogLevels.map(_.toString).mkString("|")}]"
        )
        .optional()
        .validate(x =>
          Either.cond(
            LogLevels.map(_.toString).contains(x.toUpperCase),
            (),
            s"Must be between one of ${LogLevels.map(_.toString)}"
          )
        ),
      cmd("service")
        .text("run the checkpointing service")
        .action((_, opts) => opts.copy(mode = Service(DefaultName)))
        .children(
          opt[String]('n', "node-name")
            .action((x, opts) => opts.mapService(_.copy(nodeName = x)))
            .text("unique name for the node")
            .required()
        ),
      cmd("keygen")
        .text("generate an ECDSA key pair")
        .action((_, opts) => opts.copy(mode = KeyGen))
    )
  }
}
