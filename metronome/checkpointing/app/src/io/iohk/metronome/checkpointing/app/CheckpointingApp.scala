package io.iohk.metronome.checkpointing.app

import cats.effect.ExitCode
import monix.eval.{Task, TaskApp}
import io.iohk.metronome.checkpointing.app.config.{
  CheckpointingConfig,
  CheckpointingConfigParser,
  CheckpointingOptions
}

object CheckpointingApp extends TaskApp {
  override def run(args: List[String]): Task[ExitCode] = {
    CheckpointingConfigParser.parse match {
      case Left(error) =>
        Task
          .delay(println(s"Error parsing configuration: $error"))
          .as(ExitCode.Error)
      case Right(config) =>
        CheckpointingOptions.parse(config, args) match {
          case None =>
            Task.pure(ExitCode.Error)
          case Some(opts) =>
            setLogProperties(opts) >>
              run(opts, config)
        }
    }
  }

  def run(
      opts: CheckpointingOptions,
      config: CheckpointingConfig
  ): Task[ExitCode] =
    CheckpointingComposition
      .compose(opts, config)
      .use(_ => Task.never.as(ExitCode.Success))

  /** Set dynamic system properties expected by `logback.xml` before any logging module is loaded. */
  def setLogProperties(opts: CheckpointingOptions): Task[Unit] = Task {
    // Separate log file for each node.
    System.setProperty("log.file.name", opts.nodeName)
    // Control how much logging goes on the console.
    System.setProperty("log.console.level", opts.logLevel.toString)
  }.void
}
