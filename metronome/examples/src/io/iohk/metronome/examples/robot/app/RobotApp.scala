package io.iohk.metronome.examples.robot.app

import cats.effect.ExitCode
import monix.eval.{Task, TaskApp}
import scopt.OParser
import io.iohk.metronome.examples.robot.app.config.{
  RobotConfigParser,
  RobotConfig
}

object RobotApp extends TaskApp {
  case class CommandLineOptions(
      nodeIndex: Int = 0
  )

  def oparser(config: RobotConfig) = {
    val builder = OParser.builder[CommandLineOptions]
    import builder._

    OParser.sequence(
      programName("robot"),
      opt[Int]('i', "node-index")
        .action((i, opts) => opts.copy(nodeIndex = i))
        .text("index of example node to run")
        .required()
        .validate(i =>
          Either.cond(
            0 <= i && i < config.nodes.length,
            (),
            s"Must be between 0 and ${config.nodes.length - 1}"
          )
        )
    )
  }

  override def run(args: List[String]): Task[ExitCode] = {
    RobotConfigParser.parse match {
      case Left(error) =>
        Task.delay(println(error)).as(ExitCode.Error)
      case Right(config) =>
        OParser.parse(oparser(config), args, CommandLineOptions()) match {
          case None =>
            Task.pure(ExitCode.Error)
          case Some(opts) =>
            run(opts, config)
        }
    }
  }

  def run(opts: CommandLineOptions, config: RobotConfig): Task[ExitCode] = ???

}
