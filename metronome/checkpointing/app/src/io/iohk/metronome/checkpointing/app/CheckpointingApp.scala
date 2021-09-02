package io.iohk.metronome.checkpointing.app

import cats.effect.ExitCode
import com.typesafe.config.ConfigFactory
import monix.eval.{Task, TaskApp}
import io.iohk.metronome.checkpointing.app.config.{
  CheckpointingConfigParser,
  CheckpointingOptions
}
import io.iohk.metronome.checkpointing.app.config.CheckpointingConfig

object CheckpointingApp extends TaskApp {
  override def run(args: List[String]): Task[ExitCode] = {
    CheckpointingOptions.parse(args) match {
      case None =>
        Task.pure(ExitCode.Error)

      case Some(opts) =>
        run(opts)
    }
  }

  def run(opts: CheckpointingOptions): Task[ExitCode] =
    opts.mode match {
      case CheckpointingOptions.KeyGen =>
        setLogProperties(opts, "keygen") >>
          // Not parsing the configuration for this as it may be incomplete without the keys.
          CheckpointingKeyGen.generateAndPrint.as(ExitCode.Success)

      case CheckpointingOptions.Service =>
        CheckpointingConfigParser.parse(ConfigFactory.load()) match {
          case Left(error) =>
            Task
              .delay(println(s"Error parsing configuration: $error"))
              .as(ExitCode.Error)

          case Right(config) =>
            setLogProperties(opts, config.name) >>
              CheckpointingComposition
                .compose(config)
                .use(_ => Task.never.as(ExitCode.Success))
        }
    }

  /** Set dynamic system properties expected by `logback.xml` before any logging module is loaded. */
  def setLogProperties(
      opts: CheckpointingOptions,
      name: String
  ): Task[Unit] = Task {
    // Separate log file for each node.
    System.setProperty("log.file.name", name)
    // Control how much logging goes on the console.
    System.setProperty("log.console.level", opts.logLevel.toString)
  }.void
}
