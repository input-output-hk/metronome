package io.iohk.metronome.hotstuff.service

import cats.implicits._
import cats.Parallel
import cats.effect.{Sync, Resource, Concurrent, ContextShift, Timer}
import cats.effect.concurrent.Ref
import io.iohk.metronome.core.fibers.FiberMap
import io.iohk.metronome.core.messages.{
  RPCMessageCompanion,
  RPCPair,
  RPCTracker
}
import io.iohk.metronome.hotstuff.consensus.{Federation, ViewNumber}
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  ProtocolState,
  Block,
  Signing
}
import io.iohk.metronome.hotstuff.service.messages.SyncMessage
import io.iohk.metronome.hotstuff.service.pipes.SyncPipe
import io.iohk.metronome.hotstuff.service.storage.{
  BlockStorage,
  ViewStateStorage
}
import io.iohk.metronome.hotstuff.service.sync.{
  BlockSynchronizer,
  ViewSynchronizer
}
import io.iohk.metronome.hotstuff.service.tracing.SyncTracers
import io.iohk.metronome.networking.ConnectionHandler
import io.iohk.metronome.storage.{KVStoreRunner, KVStore}
import scala.util.control.NonFatal
import scala.concurrent.duration._
import scala.reflect.ClassTag

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
class SyncService[F[_]: Concurrent, N, A <: Agreement: Block](
    publicKey: A#PKey,
    network: Network[F, A, SyncMessage[A]],
    blockStorage: BlockStorage[N, A],
    viewStateStorage: ViewStateStorage[N, A],
    syncPipe: SyncPipe[F, A]#Right,
    getState: F[ProtocolState[A]],
    incomingFiberMap: FiberMap[F, A#PKey],
    rpcTracker: RPCTracker[F, SyncMessage[A]]
)(implicit tracers: SyncTracers[F, A], storeRunner: KVStoreRunner[F, N]) {
  import SyncMessage._
  import SyncService.SyncMode

  type SyncModeRef = Ref[F, SyncMode[F, N, A]]

  private def protocolStatus: F[Status[A]] =
    getState.map { state =>
      Status(state.viewNumber, state.prepareQC, state.commitQC)
    }

  /** Request a block from a peer. */
  private def getBlock(from: A#PKey, blockHash: A#Hash): F[Option[A#Block]] = {
    val request = GetBlockRequest(RequestId(), blockHash)
    sendRequest(from, request) map (_.map(_.block))
  }

  /** Request the status of a peer. */
  private def getStatus(from: A#PKey): F[Option[Status[A]]] = {
    if (from == publicKey) {
      protocolStatus.map(_.some)
    } else {
      val request = GetStatusRequest[A](RequestId())
      sendRequest(from, request) map (_.map(_.status))
    }
  }

  /** Send a request to the peer and track the response.
    *
    * Returns `None` if we're not connected or the request times out.
    */
  private def sendRequest[
      Req <: RPCMessageCompanion#Request,
      Res <: RPCMessageCompanion#Response
  ](from: A#PKey, request: Req)(implicit
      ev1: Req <:< SyncMessage[A] with SyncMessage.Request,
      ev2: RPCPair.Aux[Req, Res],
      ct: ClassTag[Res]
  ): F[Option[Res]] = {
    for {
      join <- rpcTracker.register[Req, Res](request)
      _    <- network.sendMessage(from, request)
      res  <- join
      _    <- tracers.requestTimeout(from -> request).whenA(res.isEmpty)
    } yield res
  }

  /** Process incoming network messages. */
  private def processNetworkMessages: F[Unit] = {
    // TODO (PM-3186): Rate limiting per source.
    network.incomingMessages
      .mapEval[Unit] { case ConnectionHandler.MessageReceived(from, message) =>
        // Handle on a fiber dedicated to the source.
        incomingFiberMap
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
        protocolStatus.flatMap { status =>
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
        rpcTracker.complete(response).flatMap { ok =>
          tracers.responseIgnored(from -> response).whenA(!ok)
        }
    }

    process.handleErrorWith { case NonFatal(ex) =>
      tracers.error(ex)
    }
  }

  /** Read Requests from the SyncPipe and send Responses.
    *
    * These are coming from the `ConsensusService` asking for a
    * `Prepare` message to be synchronized with the sender, or
    * for the view to be synchronized with the whole federation.
    */
  private def processSyncPipe(
      syncModeFactory: SyncMode.Factory[F, N, A]
  ): Resource[F, Unit] =
    for {
      // Initialize the sync mode from view number 0, which accepts
      // any requests. Make sure the synchronizer is shut down at
      // the end, if it's running.
      syncModeRef <- Resource.make(
        syncModeFactory
          .block(ViewNumber(0))
          .flatMap(Ref.of[F, SyncMode[F, N, A]](_))
      ) { syncModeRef =>
        syncModeRef.get.flatMap {
          case SyncMode.View(_)                          => ().pure[F]
          case SyncMode.Block(_, _, fiberMapShutdown, _) => fiberMapShutdown
        }
      }

      // Start processing messages from the pipe.
      _ <- Concurrent[F].background {
        syncPipe.receive
          .mapEval[Unit] {
            case request @ SyncPipe.PrepareRequest(_, _) =>
              handlePrepareRequest(syncModeRef, request)

            case request @ SyncPipe.StatusRequest(_) =>
              handleStatusRequest(
                syncModeRef,
                syncModeFactory,
                request
              )
          }
          .completedL
      }
    } yield ()

  /** Sync with the sender up to the High Q.C. it sent, then validate the prepared block. */
  private def handlePrepareRequest(
      syncModeRef: SyncModeRef,
      request: SyncPipe.PrepareRequest[A]
  ): F[Unit] = {
    syncModeRef.get.flatMap {
      case SyncMode.View(_) =>
        // We're in the process of syncing the view, so we can ignore any further
        // blocks until we switch back.
        ().pure[F]

      case SyncMode.Block(blockSynchronizer, syncFiberMap, _, _) =>
        val sender  = request.sender
        val prepare = request.prepare
        // It is enough to respond to the last block positively, it will indicate
        // that the whole range can be executed later (at that point from storage).
        // If the same leader is sending us newer proposals, we can ignore the
        // previous pepared blocks - they are either part of the new Q.C.,
        // in which case they don't need to be validated, or they have not
        // gathered enough votes, and been superseded by a new proposal.
        syncFiberMap.cancelQueue(sender) >>
          syncFiberMap
            .submit(sender) {
              for {
                _       <- blockSynchronizer.sync(sender, prepare.highQC)
                isValid <- validateBlock(prepare.block)
                _       <- syncPipe.send(SyncPipe.PrepareResponse(request, isValid))
              } yield ()
            }
            .attemptNarrow[FiberMap.ShutdownException] // Ignore shutdowns
            .void
    }
  }

  /** Get the latest status of federation members, download the corresponding block
    * and prune all existing block history, making the latest Commit Q.C. the new
    * root in the block tree.
    */
  private def handleStatusRequest(
      syncModeRef: SyncModeRef,
      syncModeFactory: SyncMode.Factory[F, N, A],
      request: SyncPipe.StatusRequest
  ): F[Unit] = {
    syncModeRef.get.flatMap {
      case SyncMode.View(_) =>
        // We're already syncing the view, so we can ignore any further requests.
        ().pure[F]

      case SyncMode.Block(_, _, _, lastSyncedViewNumber)
          if lastSyncedViewNumber >= request.viewNumber =>
        // We have already synced to a view that covers the state where the request was made.
        ().pure[F]

      case SyncMode.Block(_, _, shutdownBlockSync, lastSyncedViewNumber) =>
        for {
          // Switch to view sync mode
          viewMode <- syncModeFactory.view
          _        <- syncModeRef.set(viewMode)
          // Cancel all outstanding block syncing.
          _ <- shutdownBlockSync
          // Perform the rest in the background, so we keep processing the pipe.
          _ <- Concurrent[F].start {
            val task = for {
              // Sync to the latest Commit Q.C.
              federationStatus <- viewMode.synchronizer.sync
              status = federationStatus.status
              blockMode <- syncModeFactory.block(status.viewNumber)

              // Download the block in the Commit Q.C.
              block <- blockMode.synchronizer.downloadBlockInQC(
                federationStatus.sources,
                status.commitQC
              )

              // Prune the block store from earlier blocks that are no longer traversable.
              _ <- fastForwardStorage(status, block)

              // Sync any application specific state, e.g. a ledger.
              _ <- syncAppState(status.commitQC.blockHash)

              // Switch back to block sync mode
              _ <- syncModeRef.set(blockMode)
              _ <- syncPipe.send(SyncPipe.StatusResponse(status))
            } yield ()

            task.handleErrorWith { case NonFatal(ex) =>
              // Restore block syncing, so we don't get stuck.
              tracers.error(ex) >>
                syncModeFactory
                  .block(lastSyncedViewNumber)
                  .flatMap(syncModeRef.set)
            }
          }
        } yield ()
    }
  }

  /** Replace the state we have persisted with what we synced with the federation.
    *
    * Prunes old blocks, the Commit Q.C. will be the new root.
    */
  private def fastForwardStorage(status: Status[A], block: A#Block): F[Unit] = {
    val blockHash = Block[A].blockHash(block)
    assert(blockHash == status.commitQC.blockHash)

    val query: KVStore[N, Unit] =
      for {
        viewState <- viewStateStorage.getBundle.lift
        // Insert the new block.
        _ <- blockStorage.put(block)

        // Prune old data, but keep the new block.
        ds <- blockStorage
          .getDescendants(
            viewState.rootBlockHash,
            skip = Set(blockHash)
          )
          .lift
        _ <- ds.traverse(blockStorage.deleteUnsafe(_))

        // Considering the committed block as executed, we have its state already.
        _ <- viewStateStorage.setLastExecutedBlockHash(blockHash)
        _ <- viewStateStorage.setRootBlockHash(blockHash)
        // The rest of the fields will be set by the ConsensusService.
      } yield ()

    storeRunner.runReadWrite(query)
  }

  // TODO (PM-3132, PM-3133): Block validation.
  private def validateBlock(block: A#Block): F[Boolean] = true.pure[F]

  // TODO (PM-3135): Tell the application to sync state of the block.
  private def syncAppState(blockHash: A#Hash): F[Unit] = ().pure[F]
}

object SyncService {

  /** Create a `SyncService` instance and start processing messages
    * in the background, shutting processing down when the resource is
    * released.
    */
  def apply[F[
      _
  ]: Concurrent: ContextShift: Timer: Parallel, N, A <: Agreement: Block: Signing](
      publicKey: A#PKey,
      federation: Federation[A#PKey],
      network: Network[F, A, SyncMessage[A]],
      blockStorage: BlockStorage[N, A],
      viewStateStorage: ViewStateStorage[N, A],
      syncPipe: SyncPipe[F, A]#Right,
      getState: F[ProtocolState[A]],
      timeout: FiniteDuration = 10.seconds
  )(implicit
      tracers: SyncTracers[F, A],
      storeRunner: KVStoreRunner[F, N]
  ): Resource[F, SyncService[F, N, A]] =
    // TODO (PM-3186): Add capacity as part of rate limiting.
    for {
      incomingFiberMap <- FiberMap[F, A#PKey]()
      rpcTracker <- Resource.liftF {
        RPCTracker[F, SyncMessage[A]](timeout)
      }
      service = new SyncService(
        publicKey,
        network,
        blockStorage,
        viewStateStorage,
        syncPipe,
        getState,
        incomingFiberMap,
        rpcTracker
      )

      syncModeFactory = new SyncMode.Factory[F, N, A] {
        override def block(viewNumber: ViewNumber) =
          for {
            (syncFiberMap, syncFiberMapRelease) <-
              FiberMap[F, A#PKey]().allocated

            blockSynchronizer <- BlockSynchronizer[F, N, A](
              blockStorage,
              service.getBlock
            )

            mode = SyncMode.Block(
              blockSynchronizer,
              syncFiberMap,
              syncFiberMapRelease,
              viewNumber
            )
          } yield mode

        override val view =
          Sync[F]
            .delay {
              new ViewSynchronizer[F, A](federation, service.getStatus)
            }
            .map(SyncMode.View(_))

      }

      _ <- Concurrent[F].background(service.processNetworkMessages)
      _ <- service.processSyncPipe(syncModeFactory)
    } yield service

  /** The `SyncService` can be in two modes: either we're in sync with the federation
    * and downloading the odd missing block every now and then, or we are out of sync,
    * in which case we need to ask everyone to find out what the current view number
    * is, and then jump straight to the latest Commit Quorum Certificate.
    *
    * Our implementation assumes that this is always supported by the application.
    */
  sealed trait SyncMode[F[_], N, A <: Agreement]

  object SyncMode {
    case class Block[F[_], N, A <: Agreement](
        synchronizer: BlockSynchronizer[F, N, A],
        fiberMap: FiberMap[F, A#PKey],
        fiberMapShutdown: F[Unit],
        lastSyncedViewNumber: ViewNumber
    ) extends SyncMode[F, N, A]

    case class View[F[_], N, A <: Agreement](
        synchronizer: ViewSynchronizer[F, A]
    ) extends SyncMode[F, N, A]

    trait Factory[F[_], N, A <: Agreement] {
      def block(viewNumber: ViewNumber): F[Block[F, N, A]]
      def view: F[View[F, N, A]]
    }
  }
}
