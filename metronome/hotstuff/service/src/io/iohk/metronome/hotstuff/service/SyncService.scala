package io.iohk.metronome.hotstuff.service

import cats.implicits._
import cats.Parallel
import cats.effect.{Sync, Resource, Concurrent, ContextShift, Timer}
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
import io.iohk.metronome.networking.{ConnectionHandler, Network}
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
class SyncService[F[_]: Concurrent: ContextShift, N, A <: Agreement: Block](
    publicKey: A#PKey,
    network: Network[F, A#PKey, SyncMessage[A]],
    appService: ApplicationService[F, A],
    blockStorage: BlockStorage[N, A],
    viewStateStorage: ViewStateStorage[N, A],
    syncPipe: SyncPipe[F, A]#Right,
    getState: F[ProtocolState[A]],
    incomingFiberMap: FiberMap[F, A#PKey],
    rpcTracker: RPCTracker[F, SyncMessage[A]]
)(implicit tracers: SyncTracers[F, A], storeRunner: KVStoreRunner[F, N]) {
  import SyncMessage._

  type BlockSync = SyncService.BlockSynchronizerWithFiberMap[F, N, A]

  private def protocolStatus: F[Status[A]] =
    getState.map { state =>
      Status(state.viewNumber, state.prepareQC, state.commitQC)
    }

  /** Request a block from a peer. */
  private def getBlock(from: A#PKey, blockHash: A#Hash): F[Option[A#Block]] = {
    for {
      requestId <- RequestId[F]
      request = GetBlockRequest(requestId, blockHash)
      maybeResponse <- sendRequest(from, request)
    } yield maybeResponse.map(_.block)
  }

  /** Request the status of a peer. */
  private def getStatus(from: A#PKey): F[Option[Status[A]]] =
    if (from == publicKey) {
      protocolStatus.map(_.some)
    } else {
      for {
        requestId <- RequestId[F]
        request = GetStatusRequest[A](requestId)
        maybeResponse <- sendRequest(from, request)
      } yield maybeResponse.map(_.status)
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
        rpcTracker.complete(response).flatMap {
          case Right(ok) =>
            tracers.responseIgnored((from, response, None)).whenA(!ok)
          case Left(ex) =>
            tracers.responseIgnored((from, response, Some(ex)))
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
      makeBlockSync: F[BlockSync],
      viewSynchronizer: ViewSynchronizer[F, A]
  ): F[Unit] =
    syncPipe.receive.consume.use { consumer =>
      def loop(
          blockSync: BlockSync,
          lastSyncedViewNumber: ViewNumber
      ): F[Unit] = {
        consumer.pull.flatMap {
          case Right(SyncPipe.PrepareRequest(_, prepare))
              if prepare.viewNumber < lastSyncedViewNumber =>
            // We have already synced to a Commit Q.C. higher than this old PrepareRequest.
            loop(blockSync, lastSyncedViewNumber)

          case Right(SyncPipe.StatusRequest(viewNumber))
              if viewNumber < lastSyncedViewNumber =>
            // We have already synced higher than this old StatusRequest.
            loop(blockSync, lastSyncedViewNumber)

          case Right(request @ SyncPipe.PrepareRequest(_, _)) =>
            handlePrepareRequest(blockSync, request) >>
              loop(blockSync, lastSyncedViewNumber)

          case Right(request @ SyncPipe.StatusRequest(_)) =>
            handleStatusRequest(
              makeBlockSync,
              blockSync,
              viewSynchronizer,
              request
            ).flatMap {
              (loop _).tupled
            }

          case Left(maybeError) =>
            blockSync.fiberMapRelease >>
              maybeError.fold(().pure[F])(Sync[F].raiseError(_))
        }
      }

      makeBlockSync.flatMap { blockSync =>
        loop(blockSync, ViewNumber(0))
      }
    }

  /** Sync with the sender up to the High Q.C. it sent, then validate the prepared block.
    *
    * This is done in the background, while further requests are taken from the pipe.
    */
  private def handlePrepareRequest(
      blockSync: BlockSync,
      request: SyncPipe.PrepareRequest[A]
  ): F[Unit] = {
    val sender  = request.sender
    val prepare = request.prepare
    // It is enough to respond to the last block positively, it will indicate
    // that the whole range can be executed later (at that point from storage).
    // If the same leader is sending us newer proposals, we can ignore the
    // previous pepared blocks - they are either part of the new Q.C.,
    // in which case they don't need to be validated, or they have not
    // gathered enough votes, and been superseded by a new proposal.
    blockSync.fiberMap.cancelQueue(sender) >>
      blockSync.fiberMap
        .submit(sender) {
          blockSync.synchronizer.sync(sender, prepare.highQC) >>
            appService.validateBlock(prepare.block) >>= {
            case Some(isValid) =>
              syncPipe.send(SyncPipe.PrepareResponse(request, isValid))
            case None =>
              // We didn't have data to decide validity in time; not responding.
              ().pure[F]
          }
        }
        .void
  }

  /** Shut down the any outstanding block downloads, sync the view,
    * then create another block synchronizer instance to resume with.
    */
  private def handleStatusRequest(
      makeBlockSync: F[BlockSync],
      blockSync: BlockSync,
      viewSynchronizer: ViewSynchronizer[F, A],
      request: SyncPipe.StatusRequest
  ): F[(BlockSync, ViewNumber)] =
    for {
      // Cancel all outstanding block syncing.
      _ <- blockSync.fiberMapRelease
      // The block synchronizer is still usable.
      viewNumber <- syncStatus(
        blockSync.synchronizer,
        viewSynchronizer
      ).handleErrorWith { case NonFatal(ex) =>
        tracers.error(ex).as(request.viewNumber)
      }
      // Create a fresh fiber and block synchronizer instance.
      // When the previous goes out of scope, its ephemeral storage is freed.
      newBlockSync <- makeBlockSync
    } yield (newBlockSync, viewNumber)

  /** Get the latest status of federation members, download the corresponding block
    * and prune all existing block history, making the latest Commit Q.C. the new
    * root in the block tree.
    *
    * This is done in the foreground, no further requests are taken from the pipe.
    */
  private def syncStatus(
      blockSynchronizer: BlockSynchronizer[F, N, A],
      viewSynchronizer: ViewSynchronizer[F, A]
  ): F[ViewNumber] =
    for {
      // Sync to the latest Commit Q.C.
      federationStatus <- viewSynchronizer.sync
      status = federationStatus.status

      // Download the block in the Commit Q.C.
      block <- blockSynchronizer
        .getBlockFromQuorumCertificate(
          federationStatus.sources,
          status.commitQC
        )
        .rethrow

      // Sync any application specific state, e.g. a ledger.
      // Do this before we prune the existing blocks and set the new root.
      _ <- appService.syncState(federationStatus.sources, block)

      // Prune the block store from earlier blocks that are no longer traversable.
      _ <- fastForwardStorage(status, block)

      // Tell the ConsensusService about the new Status.
      _ <- syncPipe.send(SyncPipe.StatusResponse(status))
    } yield status.viewNumber

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
}

object SyncService {

  /** Create a `SyncService` instance and start processing messages
    * in the background, shutting processing down when the resource is
    * released.
    */
  def apply[
      F[_]: Concurrent: ContextShift: Timer: Parallel,
      N,
      A <: Agreement: Block: Signing
  ](
      publicKey: A#PKey,
      federation: Federation[A#PKey],
      network: Network[F, A#PKey, SyncMessage[A]],
      appService: ApplicationService[F, A],
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
        appService,
        blockStorage,
        viewStateStorage,
        syncPipe,
        getState,
        incomingFiberMap,
        rpcTracker
      )

      blockSync = for {
        (syncFiberMap, syncFiberMapRelease) <- FiberMap[F, A#PKey]().allocated
        blockSynchronizer <- BlockSynchronizer[F, N, A](
          publicKey,
          federation,
          blockStorage,
          service.getBlock
        )
      } yield BlockSynchronizerWithFiberMap(
        blockSynchronizer,
        syncFiberMap,
        syncFiberMapRelease
      )

      viewSynchronizer = new ViewSynchronizer[F, A](
        federation,
        service.getStatus
      )

      _ <- Concurrent[F].background {
        service.processNetworkMessages
      }
      _ <- Concurrent[F].background {
        service.processSyncPipe(blockSync, viewSynchronizer)
      }
    } yield service

  /** The `SyncService` can be in two modes: either we're in sync with the federation
    * and downloading the odd missing block every now and then, or we are out of sync,
    * in which case we need to ask everyone to find out what the current view number
    * is, and then jump straight to the latest Commit Quorum Certificate.
    *
    * Our implementation assumes that this is always supported by the application.
    *
    * When we go from block sync to view sync, the block syncs happening in the
    * background on the fiber ap in this class are canceled, and the synchronizer
    * instance with its ephemeral storage is discarded.
    */
  case class BlockSynchronizerWithFiberMap[F[_], N, A <: Agreement](
      synchronizer: BlockSynchronizer[F, N, A],
      fiberMap: FiberMap[F, A#PKey],
      fiberMapRelease: F[Unit]
  )
}
