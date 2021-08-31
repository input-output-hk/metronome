package io.iohk.metronome.checkpointing.app

import io.iohk.metronome.checkpointing.app.config.{
  CheckpointingConfig,
  CheckpointingOptions
}
import cats.effect.Resource
import monix.eval.Task

trait CheckpointingComposition {
  def compose(
      opts: CheckpointingOptions,
      config: CheckpointingConfig
  ): Resource[Task, Unit] = ???
}

object CheckpointingComposition extends CheckpointingComposition