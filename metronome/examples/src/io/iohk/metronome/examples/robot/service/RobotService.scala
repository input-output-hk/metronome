package io.iohk.metronome.examples.robot.service

import cats.implicits._
import cats.effect.{Sync, Timer}
import cats.data.{NonEmptyList, NonEmptyVector}
import io.iohk.metronome.crypto.ECPublicKey
import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.hotstuff.service.ApplicationService
import io.iohk.metronome.examples.robot.RobotAgreement
import io.iohk.metronome.examples.robot.models.{RobotBlock, Robot}
import io.iohk.metronome.examples.robot.service.messages.RobotMessage
import io.iohk.metronome.hotstuff.consensus.basic.QuorumCertificate
import io.iohk.metronome.hotstuff.service.Network
import io.iohk.metronome.hotstuff.service.storage.{
  BlockStorage,
  ViewStateStorage
}
import io.iohk.metronome.storage.{KVStoreRunner, KVRingBuffer}
import io.iohk.metronome.storage.KVStoreRead
import scala.util.Random
import scala.concurrent.duration._

class RobotService[F[_]: Sync: Timer, N](
    maxRow: Int,
    maxCol: Int,
    network: Network[F, RobotAgreement, RobotMessage],
    blockStorage: BlockStorage[N, RobotAgreement],
    viewStateStorage: ViewStateStorage[N, RobotAgreement],
    stateStorage: KVRingBuffer[N, Hash, Robot.State],
    simulatedDecisionTime: FiniteDuration = 1.second
)(implicit storeRunner: KVStoreRunner[F, N])
    extends ApplicationService[F, RobotAgreement] {

  /** Make a random valid move on top of the last block. */
  override def createBlock(
      highQC: QuorumCertificate[RobotAgreement]
  ): F[Option[RobotBlock]] = {
    projectState(highQC.blockHash).flatMap {
      case None =>
        none.pure[F]

      case Some(preState) =>
        // Make a valid move; we're not validating our own blocks.
        // Insert some artifical delay otherwise it will churn out
        // blocks as fast as it can.
        Timer[F].sleep(simulatedDecisionTime) >>
          advanceState(preState).map { case (command, postState) =>
            RobotBlock(
              parentHash = highQC.blockHash,
              postStateHash = postState.hash,
              command = command
            ).some
          }
    }
  }

  /** Project state from the last executed block hash to a certain descendant.
    * Returns None if we don't have the block.
    */
  private def projectState(blockHash: Hash): F[Option[Robot.State]] = {
    val query: KVStoreRead[N, (Option[Robot.State], List[RobotBlock])] = for {
      lastExecutedBlockHash <- viewStateStorage.getLastExecutedBlockHash
      blockPath <- blockStorage.getPathFromAncestor(
        lastExecutedBlockHash,
        blockHash
      )
      blocks <- blockPath.traverse(blockStorage.get).map(_.flatten)
      pendingBlocks = blocks.drop(1)
      maybeLastExecutedState <- blocks match {
        case lastExecutedBlock :: _
            if lastExecutedBlock.hash == lastExecutedBlockHash =>
          stateStorage.get(lastExecutedBlock.postStateHash)
        case _ =>
          KVStoreRead.instance[N].pure(None)
      }
    } yield (maybeLastExecutedState, pendingBlocks)

    storeRunner.runReadOnly(query).map {
      case (maybeLastExecutedState, pendingBlocks) =>
        maybeLastExecutedState.map { lastExecutedState =>
          pendingBlocks
            .foldLeft(lastExecutedState) { case (state, block) =>
              state.update(block.command)
            }
        }
    }
  }

  /** Make a valid move. */
  private def advanceState(
      preState: Robot.State
  ): F[(Robot.Command, Robot.State)] = {
    import Robot.Command._
    Sync[F]
      .delay(Random.nextDouble())
      .map {
        case d if d <= 0.65 => MoveForward
        case d if d <= 0.80 => TurnLeft
        case d if d <= 0.95 => TurnRight
        case _              => Rest
      }
      .flatMap { command =>
        preState.update(command) match {
          case postState if isValid(postState) =>
            (command, postState).pure[F]
          case _ =>
            advanceState(preState)
        }
      }
  }

  /** Keep the robot within the agreed area. */
  private def isValid(state: Robot.State): Boolean =
    state.position.row >= 0 &&
      state.position.col >= 0 &&
      state.position.row <= maxRow &&
      state.position.col <= maxCol

  /** Validate a block by executing any pending changes and checking the post state. */
  override def validateBlock(block: RobotBlock): F[Boolean] =
    projectState(block.parentHash).map {
      case None =>
        false
      case Some(preState) =>
        val postState = preState.update(block.command)
        postState.hash == block.postStateHash && isValid(postState)
    }

  /** Execute the next block in the queue, store the resulting state. */
  override def executeBlock(
      block: RobotBlock,
      commitQC: QuorumCertificate[RobotAgreement],
      commitPath: NonEmptyList[RobotAgreement.Hash]
  ): F[Unit] =
    projectState(block.parentHash).flatMap {
      case None =>
        Sync[F].raiseError(new IllegalStateException("Can't find pre-state."))
      case Some(preState) =>
        storeRunner.runReadWrite {
          val postState = preState.update(block.command)
          stateStorage.put(postState.hash, postState)
        }
    }

  def syncState(
      sources: NonEmptyVector[ECPublicKey],
      block: RobotBlock
  ): F[Unit] = ???

}
