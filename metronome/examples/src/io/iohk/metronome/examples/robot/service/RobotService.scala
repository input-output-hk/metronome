package io.iohk.metronome.examples.robot.service

import cats.implicits._
import cats.effect.{Sync, Timer, Resource, Concurrent}
import cats.data.{NonEmptyList, NonEmptyVector, OptionT}
import io.iohk.metronome.core.messages.{RPCTracker, RPCSupport}
import io.iohk.metronome.crypto.ECPublicKey
import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.examples.robot.RobotAgreement
import io.iohk.metronome.examples.robot.models.{RobotBlock, Robot}
import io.iohk.metronome.examples.robot.service.messages.RobotMessage
import io.iohk.metronome.examples.robot.service.tracing.RobotTracers
import io.iohk.metronome.hotstuff.consensus.basic.{QuorumCertificate, Phase}
import io.iohk.metronome.hotstuff.service.ApplicationService
import io.iohk.metronome.hotstuff.service.storage.{
  BlockStorage,
  ViewStateStorage
}
import io.iohk.metronome.networking.{ConnectionHandler, Network}
import io.iohk.metronome.storage.{KVStoreRunner, KVRingBuffer}
import io.iohk.metronome.storage.KVStoreRead
import scala.util.Random
import scala.concurrent.duration._
import cats.effect.Concurrent

class RobotService[F[_]: Sync: Timer, N](
    maxRow: Int,
    maxCol: Int,
    publicKey: RobotAgreement.PKey,
    network: Network[F, RobotAgreement.PKey, RobotMessage],
    blockStorage: BlockStorage[N, RobotAgreement],
    viewStateStorage: ViewStateStorage[N, RobotAgreement],
    stateStorage: KVRingBuffer[N, Hash, Robot.State],
    rpcTracker: RPCTracker[F, RobotMessage],
    simulatedDecisionTime: FiniteDuration
)(implicit storeRunner: KVStoreRunner[F, N], tracers: RobotTracers[F])
    extends RPCSupport[
      F,
      RobotAgreement.PKey,
      RobotMessage,
      RobotMessage with RobotMessage.Request,
      RobotMessage with RobotMessage.Response
    ](rpcTracker, RobotMessage.RequestId[F])
    with ApplicationService[F, RobotAgreement] {

  protected override val sendRequest = (to, req) => network.sendMessage(to, req)

  /** Make a random valid move on top of the last block. */
  override def createBlock(
      highQC: QuorumCertificate[RobotAgreement, Phase.Prepare]
  ): F[Option[RobotBlock]] = {
    val parentState = for {
      parent <- OptionT {
        storeRunner.runReadOnly {
          blockStorage.get(highQC.blockHash)
        }
      }
      preState <- OptionT(projectState(highQC.blockHash))
    } yield (parent, preState)

    parentState
      .semiflatMap[RobotBlock] { case (parent, preState) =>
        // Make a valid move; we're not validating our own blocks.
        // Insert some artifical delay otherwise it will churn out
        // blocks as fast as it can.
        for {
          _                    <- Timer[F].sleep(simulatedDecisionTime)
          (command, postState) <- advanceState(preState)
          block = RobotBlock(
            parentHash = parent.hash,
            height = parent.height + 1,
            postStateHash = postState.hash,
            command = command
          )
          _ <- tracers.proposing(block)
        } yield block
      }
      .value
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
  override def validateBlock(block: RobotBlock): F[Option[Boolean]] =
    projectState(block.parentHash)
      .map {
        case None =>
          false
        case Some(preState) =>
          val postState = preState.update(block.command)
          postState.hash == block.postStateHash && isValid(postState)
      }
      .map(_.some)

  /** Execute the next block in the queue, store the resulting state. */
  override def executeBlock(
      block: RobotBlock,
      commitQC: QuorumCertificate[RobotAgreement, Phase.Commit],
      commitPath: NonEmptyList[RobotAgreement.Hash]
  ): F[Boolean] =
    projectState(block.parentHash).flatMap {
      case None =>
        Sync[F].raiseError(new IllegalStateException("Can't find pre-state."))
      case Some(preState) =>
        storeRunner
          .runReadWrite {
            val postState = preState.update(block.command)
            stateStorage.put(postState.hash, postState).as(postState)
          }
          .flatMap { state =>
            tracers.newState(state)
          }
          .as(true)
    }

  override def syncState(
      sources: NonEmptyVector[ECPublicKey],
      block: RobotBlock
  ): F[Boolean] = {
    def loop(sources: List[ECPublicKey]): F[Unit] = {
      sources match {
        case source :: sources =>
          getState(source, block.postStateHash).flatMap {
            case None =>
              loop(sources)

            case Some(state) =>
              storeRunner.runReadWrite {
                stateStorage.put(state.hash, state).void
              }
          }
        case Nil =>
          Sync[F].raiseError(
            new IllegalStateException(
              "Could not get the state from any of the sources."
            )
          )
      }
    }

    storeRunner
      .runReadOnly {
        stateStorage.get(block.postStateHash)
      }
      .flatMap {
        case None =>
          loop(sources.toList.filterNot(_ == publicKey)).as(true)
        case Some(_) =>
          false.pure[F]
      }
  }

  private def getState(
      from: RobotAgreement.PKey,
      stateHash: Hash
  ): F[Option[Robot.State]] = {
    assert(from != publicKey, "Shouldn't try to get state from self.")

    sendRequest(from, RobotMessage.GetStateRequest(_, stateHash)).map {
      _.map(_.state).filter(_.hash == stateHash)
    }
  }

  private def processNetworkMessages: F[Unit] = {
    network.incomingMessages
      .mapEval[Unit] { case ConnectionHandler.MessageReceived(from, message) =>
        processNetworkMessage(from, message)
      }
      .completedL
  }

  private def processNetworkMessage(
      from: RobotAgreement.PKey,
      message: RobotMessage
  ): F[Unit] = {
    import RobotMessage._
    message match {
      case GetStateRequest(requestId, stateHash) =>
        storeRunner
          .runReadOnly {
            stateStorage.get(stateHash)
          }
          .flatMap {
            case None =>
              ().pure[F]
            case Some(state) =>
              network.sendMessage(from, GetStateResponse(requestId, state))
          }

      case response: RobotMessage.Response =>
        receiveResponse(from, response)
    }
  }
}

object RobotService {
  def apply[F[_]: Concurrent: Timer, N](
      maxRow: Int,
      maxCol: Int,
      publicKey: RobotAgreement.PKey,
      network: Network[F, RobotAgreement.PKey, RobotMessage],
      blockStorage: BlockStorage[N, RobotAgreement],
      viewStateStorage: ViewStateStorage[N, RobotAgreement],
      stateStorage: KVRingBuffer[N, Hash, Robot.State],
      simulatedDecisionTime: FiniteDuration = 1.second,
      timeout: FiniteDuration = 5.seconds
  )(implicit
      storeRunner: KVStoreRunner[F, N],
      tracers: RobotTracers[F]
  ): Resource[F, RobotService[F, N]] =
    for {
      rpcTracker <- Resource.liftF(RPCTracker[F, RobotMessage](timeout))
      service = new RobotService[F, N](
        maxRow,
        maxCol,
        publicKey,
        network,
        blockStorage,
        viewStateStorage,
        stateStorage,
        rpcTracker,
        simulatedDecisionTime
      )
      _ <- Concurrent[F].background {
        service.processNetworkMessages
      }
    } yield service
}
