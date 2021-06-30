package io.iohk.metronome.examples.robot.service.tracing

import io.iohk.metronome.examples.robot.models.{Robot, RobotBlock}

sealed trait RobotEvent

object RobotEvent {

  /** This node is proposing a block. */
  case class Proposing(block: RobotBlock) extends RobotEvent

  /** The federation committed to a new state. */
  case class NewState(state: Robot.State) extends RobotEvent
}
