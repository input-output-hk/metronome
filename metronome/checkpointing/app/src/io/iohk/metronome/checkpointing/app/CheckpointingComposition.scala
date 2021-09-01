package io.iohk.metronome.checkpointing.app

import io.iohk.metronome.checkpointing.app.config.CheckpointingConfig
import cats.effect.Resource
import monix.eval.Task

trait CheckpointingComposition {
  def compose(
      config: CheckpointingConfig
  ): Resource[Task, Unit] =
    ???

}

object CheckpointingComposition extends CheckpointingComposition
