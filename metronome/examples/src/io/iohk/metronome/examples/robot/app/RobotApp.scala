package io.iohk.metronome.examples.robot.app

import cats.effect.ExitCode
import monix.eval.{Task, TaskApp}
import io.iohk.metronome.examples.robot.app.config.{
  RobotConfigParser,
  RobotConfig,
  RobotOptions
}

object RobotApp extends TaskApp {

  override def run(args: List[String]): Task[ExitCode] = {
    RobotConfigParser.parse match {
      case Left(error) =>
        Task
          .delay(println(s"Error parsing configuration: $error"))
          .as(ExitCode.Error)
      case Right(config) =>
        RobotOptions.parse(config, args) match {
          case None =>
            Task.pure(ExitCode.Error)
          case Some(opts) =>
            setLogProperties(opts) >>
              run(opts, config)
        }
    }
  }

  def run(opts: RobotOptions, config: RobotConfig): Task[ExitCode] =
    RobotComposition
      .compose(opts, config)
      .use(_ => Task.never.as(ExitCode.Success))

  /** Set dynamic system properties expected by `logback.xml` before any logging module is loaded. */
  def setLogProperties(opts: RobotOptions): Task[Unit] = Task {
    // Separate log file for each node.
    System.setProperty("log.file.name", s"robot/logs/node-${opts.nodeIndex}")
    // Not logging to the console so we can display robot position.
    System.setProperty("log.console.level", s"INFO")
  }.void
}
