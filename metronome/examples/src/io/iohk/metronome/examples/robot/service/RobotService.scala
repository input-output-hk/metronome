package io.iohk.metronome.examples.robot.service

import cats.implicits._
import cats.effect.Sync
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

class RobotService[F[_]: Sync, N](
    maxRow: Int,
    maxCol: Int,
    network: Network[F, RobotAgreement, RobotMessage],
    blockStorage: BlockStorage[N, RobotAgreement],
    viewStateStorage: ViewStateStorage[N, RobotAgreement],
    stateStorage: KVRingBuffer[N, Hash, Robot.State]
)(implicit storeRunner: KVStoreRunner[F, N])
    extends ApplicationService[F, RobotAgreement] {

  override def createBlock(
      highQC: QuorumCertificate[RobotAgreement]
  ): F[Option[RobotBlock]] = {
    // Retrieve the blocks that we need to build on.
    val query: KVStoreRead[N, (Option[Robot.State], List[RobotBlock])] = for {
      lastExecutedBlockHash <- viewStateStorage.getLastExecutedBlockHash
      blockPath <- blockStorage.getPathFromAncestor(
        lastExecutedBlockHash,
        highQC.blockHash
      )
      blocks <- blockPath.traverse(blockStorage.get).map(_.flatten)
      pendingBlocks = if (blocks.nonEmpty) blocks.tail else Nil
      maybeLastExecutedState <-
        pendingBlocks.headOption match {
          case Some(lastExecutedBlock)
              if lastExecutedBlock.hash == lastExecutedBlockHash =>
            stateStorage.get(lastExecutedBlock.postStateHash)

          case _ =>
            // Shouldn't happen.
            KVStoreRead.instance[N].pure(None)
        }
    } yield (maybeLastExecutedState, pendingBlocks)

    storeRunner.runReadOnly(query).flatMap {
      case (None, _) =>
        Sync[F].raiseError(
          new IllegalStateException(
            "Cannot find state corresponding to the last executed block."
          )
        )

      case (Some(lastExecutedState), blocks) =>
        val preState = blocks
          .foldLeft(lastExecutedState) { case (state, block) =>
            state.update(block.command)
          }

        // Make a valid move; we're not validating our own blocks.
        advance(preState).map { case (command, postState) =>
          RobotBlock(
            parentHash = highQC.blockHash,
            postStateHash = postState.hash,
            command = command
          ).some
        }
    }
  }

  private def advance(
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
          case _ => advance(preState)
        }
      }
  }

  private def isValid(state: Robot.State): Boolean =
    state.position.row >= 0 &&
      state.position.col >= 0 &&
      state.position.row <= maxRow &&
      state.position.col <= maxCol

  override def validateBlock(block: RobotBlock): F[Boolean] = ???

  override def executeBlock(
      block: RobotBlock,
      commitQC: QuorumCertificate[RobotAgreement],
      commitPath: NonEmptyList[RobotAgreement.Hash]
  ): F[Unit] = ???

  def syncState(
      sources: NonEmptyVector[ECPublicKey],
      block: RobotBlock
  ): F[Unit] = ???

}
