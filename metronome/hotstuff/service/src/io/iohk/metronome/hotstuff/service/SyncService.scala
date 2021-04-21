package io.iohk.metronome.hotstuff.service

import cats.implicits._
import cats.effect.{Sync, Resource, Concurrent, ContextShift, Timer}
import io.iohk.metronome.core.fibers.{FiberMap, FiberSet}
import io.iohk.metronome.core.messages.RPCTracker
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  ProtocolState,
  Block
}
import io.iohk.metronome.hotstuff.service.messages.SyncMessage
import io.iohk.metronome.hotstuff.service.pipes.BlockSyncPipe
import io.iohk.metronome.hotstuff.service.storage.BlockStorage
import io.iohk.metronome.hotstuff.service.sync.BlockSynchronizer
import io.iohk.metronome.hotstuff.service.tracing.SyncTracers
import io.iohk.metronome.networking.ConnectionHandler
import io.iohk.metronome.storage.KVStoreRunner
import scala.util.control.NonFatal
import scala.concurrent.duration._

/** The `SyncService` handles the `SyncMessage`s coming from the network,
  * i.e. serving block and status requests, as well as receive responses
  * for outgoing requests for missing dependencies.
  *
  * It will match up the `requestId`s in the responses and discard any
  * unsolicited message.
  *
  * The block and view synchronisation components will use this service
  * to send requests to the network.
  */
class SyncService[F[_]: Sync, N, A <: Agreement](
    network: Network[F, A, SyncMessage[A]],
    blockStorage: BlockStorage[N, A],
    blockSyncPipe: BlockSyncPipe[F, A]#Right,
    getState: F[ProtocolState[A]],
    fiberMap: FiberMap[F, A#PKey],
    fiberSet: FiberSet[F],
    rpcTracker: RPCTracker[F, SyncMessage[A]]
)(implicit tracers: SyncTracers[F, A], storeRunner: KVStoreRunner[F, N]) {
  import SyncMessage._

  /** Request a block from a peer.
    *
    * Returns `None` if we're not connected or the request times out.
    */
  private def getBlock(from: A#PKey, blockHash: A#Hash): F[Option[A#Block]] = {
    val request = GetBlockRequest(RequestId(), blockHash)
    for {
      join <- rpcTracker.register(request)
      _    <- network.sendMessage(from, request)
      res  <- join
    } yield res.map(_.block)
  }

  /** Request the status of a peer.
    *
    * Returns `None` if we're not connected or the request times out.
    */
  private def getStatus(from: A#PKey): F[Option[Status[A]]] = {
    val request = GetStatusRequest[A](RequestId())
    for {
      join <- rpcTracker.register(request)
      _    <- network.sendMessage(from, request)
      res  <- join
    } yield res.map(_.status)
  }

  /** Process incoming network messages. */
  private def processNetworkMessages: F[Unit] = {
    // TODO (PM-3186): Rate limiting per source.
    network.incomingMessages
      .mapEval[Unit] { case ConnectionHandler.MessageReceived(from, message) =>
        // Handle on a fiber dedicated to the source.
        fiberMap
          .submit(from) {
            processNetworkMessage(from, message)
          }
          .attemptNarrow[FiberMap.QueueFullException]
          .flatMap {
            case Right(_) => ().pure[F]
            case Left(_)  => tracers.queueFull(from)
          }
      }
      .completedL
  }

  /** Process one incoming network message.
    *
    * It's going to be executed on a fiber.
    */
  private def processNetworkMessage(
      from: A#PKey,
      message: SyncMessage[A]
  ): F[Unit] = {
    val process = message match {
      case GetStatusRequest(requestId) =>
        getState.flatMap { state =>
          val status =
            Status(state.viewNumber, state.prepareQC, state.commitQC)

          network.sendMessage(
            from,
            GetStatusResponse(requestId, status)
          )
        }

      case GetBlockRequest(requestId, blockHash) =>
        storeRunner
          .runReadOnly {
            blockStorage.get(blockHash)
          }
          .flatMap {
            case None =>
              ().pure[F]
            case Some(block) =>
              network.sendMessage(
                from,
                GetBlockResponse(requestId, block)
              )
          }

      case response: SyncMessage.Response =>
        // TODO (PM-3063): Hand over to view synchronisation.
        // TODO (PM-3134): Hand over to block synchronisation.
        rpcTracker.complete(response).flatMap { ok =>
          tracers.responseIgnored(response).whenA(!ok)
        }
    }

    process.handleErrorWith { case NonFatal(ex) =>
      tracers.error(ex)
    }
  }

  /** Read Requests from the BlockSyncPipe and send Responses.
    *
    * These are coming from the `ConsensusService` asking for a
    * `Prepare` message to be synchronised with the sender.
    */
  def processBlockSyncPipe(
      blockSynchronizer: BlockSynchronizer[F, N, A]
  ): F[Unit] = {
    blockSyncPipe.receive
      .mapEval[Unit] { case request @ BlockSyncPipe.Request(sender, prepare) =>
        // It is enough to respond to the last block positively, it will indicate
        // that the whole range can be executed later (at that point from storage).
        fiberSet.submit {
          for {
            _       <- blockSynchronizer.sync(sender, prepare.highQC)
            isValid <- validateBlock(prepare.block)
            _       <- blockSyncPipe.send(BlockSyncPipe.Response(request, isValid))
          } yield ()
        }.void
      }
      .completedL
  }

  // TODO (PM-3132, PM-3133): Block validation.
  private def validateBlock(block: A#Block): F[Boolean] = ???
}

object SyncService {

  /** Create a `SyncService` instance and start processing messages
    * in the background, shutting processing down when the resource is
    * released.
    */
  def apply[F[_]: Concurrent: ContextShift: Timer, N, A <: Agreement: Block](
      network: Network[F, A, SyncMessage[A]],
      blockStorage: BlockStorage[N, A],
      blockSyncPipe: BlockSyncPipe[F, A]#Right,
      getState: F[ProtocolState[A]],
      timeout: FiniteDuration = 10.seconds
  )(implicit
      tracers: SyncTracers[F, A],
      storeRunner: KVStoreRunner[F, N]
  ): Resource[F, SyncService[F, N, A]] =
    // TODO (PM-3186): Add capacity as part of rate limiting.
    for {
      fiberMap <- FiberMap[F, A#PKey]()
      fiberSet <- FiberSet[F]
      rpcTracker <- Resource.liftF {
        RPCTracker[F, SyncMessage[A]](timeout)
      }
      service = new SyncService(
        network,
        blockStorage,
        blockSyncPipe,
        getState,
        fiberMap,
        fiberSet,
        rpcTracker
      )
      blockSync <- BlockSynchronizer[F, N, A](blockStorage, service.getBlock)
      _         <- Concurrent[F].background(service.processNetworkMessages)
      _         <- Concurrent[F].background(service.processBlockSyncPipe(blockSync))
    } yield service
}
