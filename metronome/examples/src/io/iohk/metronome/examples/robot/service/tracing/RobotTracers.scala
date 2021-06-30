package io.iohk.metronome.examples.robot.service.tracing

import cats.implicits._
import io.iohk.metronome.tracer.Tracer
import io.iohk.metronome.examples.robot.models.{Robot, RobotBlock}

case class RobotTracers[F[_]](
    proposing: Tracer[F, RobotBlock],
    newState: Tracer[F, Robot.State]
)

object RobotTracers {
  import RobotEvent._

  def apply[F[_]](tracer: Tracer[F, RobotEvent]): RobotTracers[F] =
    RobotTracers[F](
      proposing = tracer.contramap[RobotBlock](Proposing(_)),
      newState = tracer.contramap[Robot.State](NewState(_))
    )
}
