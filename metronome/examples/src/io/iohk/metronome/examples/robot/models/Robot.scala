package io.iohk.metronome.examples.robot.models

import io.iohk.metronome.crypto.hash.Hash

object Robot {
  sealed trait Command
  object Command {
    case object Rest        extends Command
    case object MoveForward extends Command
    case object TurnLeft    extends Command
    case object TurnRight   extends Command
  }

  case class State(
      position: Position,
      orientation: Orientation
  ) {
    import Command._

    lazy val hash: Hash =
      codecHash(this)(Codecs.robotStateCodec)

    def update(command: Command): State =
      command match {
        case Rest =>
          this
        case MoveForward =>
          copy(position = position.move(orientation))
        case TurnLeft =>
          copy(orientation = orientation.left)
        case TurnRight =>
          copy(orientation = orientation.right)
      }
  }

  case class Position(row: Int, col: Int) {
    import Orientation._

    def move(orientation: Orientation): Position =
      orientation match {
        case North => copy(row = row - 1)
        case East  => copy(col = col - 1)
        case South => copy(row = row + 1)
        case West  => copy(col = col + 1)
      }
  }

  sealed trait Orientation {
    import Orientation._

    def left: Orientation =
      this match {
        case North => West
        case East  => North
        case South => East
        case West  => South
      }

    def right: Orientation =
      this match {
        case North => East
        case East  => South
        case South => West
        case West  => North
      }
  }
  object Orientation {
    case object North extends Orientation
    case object East  extends Orientation
    case object South extends Orientation
    case object West  extends Orientation
  }
}
