package io.iohk.metronome.hotstuff.service

import cats.implicits._
import cats.effect.{Sync, Resource, Concurrent, ContextShift}
import io.iohk.metronome.core.fibers.FiberMap
import io.iohk.metronome.hotstuff.consensus.basic.{Agreement, ProtocolState}
import io.iohk.metronome.hotstuff.service.messages.SyncMessage
import io.iohk.metronome.hotstuff.service.pipes.BlockSyncPipe
import io.iohk.metronome.hotstuff.service.storage.BlockStorage
import io.iohk.metronome.hotstuff.service.tracing.SyncTracers
import io.iohk.metronome.networking.ConnectionHandler
import io.iohk.metronome.storage.KVStoreRunner
import scala.util.control.NonFatal

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
    fiberMap: FiberMap[F, A#PKey]
)(implicit tracers: SyncTracers[F, A], storeRunner: KVStoreRunner[F, N]) {

  /** Request a block from a peer.
    *
    * Returns `None` if we're not connected or the request times out.
    */
  def getBlock(from: A#PKey, blockHash: A#Hash): F[Option[A#Block]] = ???

  /** Request the status of a peer.
    *
    * Returns `None` if we're not connected or the request times out.
    */
  def getStatus(from: A#PKey): F[Option[Status[A]]] = ???

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
    import SyncMessage._

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

      case GetStatusResponse(requestId, status) =>
        // TODO (PM-3063): Hand over to view synchronisation.
        ???

      case GetBlockResponse(requestId, block) =>
        // TODO (PM-3134): Hand over to block synchronisation.
        ???
    }

    process.handleErrorWith { case NonFatal(ex) =>
      tracers.error(ex)
    }
  }

  /** Read Requests from the BlockSyncPipe and send Responses. */
  def processBlockSyncPipe: F[Unit] = {
    blockSyncPipe.receive
      .mapEval[Unit] { case request @ BlockSyncPipe.Request(sender, prepare) =>
        // TODO (PM-3134): Block sync.
        // TODO (PM-3132, PM-3133): Block validation.

        // We must take care not to insert blocks into storage and risk losing
        // the pointer to them in a restart. Maybe keep the unfinished tree
        // in memory until we find a parent we do have in storage, then
        // insert them in the opposite order, validating against the application side
        // as we go along, finally responding to the requestor.
        //
        // It is enough to respond to the last block positively, it will indicate
        // that the whole range can be executed later (at that point from storage).
        val isValid: F[Boolean] = ???

        isValid.flatMap { isValid =>
          blockSyncPipe.send(BlockSyncPipe.Response(request, isValid))
        }
      }
      .completedL
  }
}

object SyncService {

  /** Create a `SyncService` instance and start processing messages
    * in the background, shutting processing down when the resource is
    * released.
    */
  def apply[F[_]: Concurrent: ContextShift, N, A <: Agreement](
      network: Network[F, A, SyncMessage[A]],
      blockStorage: BlockStorage[N, A],
      blockSyncPipe: BlockSyncPipe[F, A]#Right,
      getState: F[ProtocolState[A]]
  )(implicit
      tracers: SyncTracers[F, A],
      storeRunner: KVStoreRunner[F, N]
  ): Resource[F, SyncService[F, N, A]] =
    // TODO (PM-3186): Add capacity as part of rate limiting.
    for {
      fiberMap <- FiberMap[F, A#PKey]()
      service = new SyncService(
        network,
        blockStorage,
        blockSyncPipe,
        getState,
        fiberMap
      )
      _ <- Concurrent[F].background(service.processNetworkMessages)
      _ <- Concurrent[F].background(service.processBlockSyncPipe)
    } yield service
}
