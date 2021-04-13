package io.iohk.metronome.hotstuff.service

import cats.implicits._
import cats.effect.{Sync, Resource, Concurrent}
import io.iohk.metronome.core.FiberPool
import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.hotstuff.service.messages.SyncMessage
import io.iohk.metronome.networking.ConnectionHandler
import cats.effect.ContextShift

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
class SyncService[F[_]: Sync, A <: Agreement](
    network: Network[F, A, SyncMessage[A]],
    syncAndValidatePipe: ConsensusService.SyncAndValidatePipe[F, A]#Right,
    consensusService: ConsensusService[F, A],
    fiberPool: FiberPool[F, A#PKey]
) {

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

  /** Process incoming network messages */
  private def processMessages: F[Unit] = {
    import SyncMessage._
    // TODO (PM-3186): Rate limiting per source.
    network.incomingMessages
      .mapEval[Unit] { case ConnectionHandler.MessageReceived(from, message) =>
        val handler: F[Unit] =
          message match {
            case GetStatusRequest(requestId) =>
              consensusService.getState.flatMap { state =>
                val status =
                  Status(state.viewNumber, state.prepareQC, state.commitQC)

                network.sendMessage(
                  from,
                  GetStatusResponse(requestId, status)
                )
              }

            case GetBlockRequest(requestId, blockHash) =>
              // TODO (PM-3134): Retreive block from storage and respond.
              val getBlock: F[Option[A#Block]] = ???

              getBlock.flatMap {
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

        // Handle on a fiber dedicated to the source.
        fiberPool
          .submit(from)(handler)
          .attemptNarrow[FiberPool.QueueFullException]
          .flatMap {
            case Right(_) => ().pure[F]
            case Left(ex) => ().pure[F] // TODO: Trace submission error.
          }
      }
      .completedL
  }

  def processSyncAndValidateRequests: F[Unit] = {
    import ConsensusService.{SyncAndValidateRequest, SyncAndValidateResponse}
    syncAndValidatePipe.receive
      .mapEval[Unit] { case request @ SyncAndValidateRequest(sender, prepare) =>
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
          syncAndValidatePipe.send(SyncAndValidateResponse(request, isValid))
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
  def apply[F[_]: Concurrent: ContextShift, A <: Agreement](
      network: Network[F, A, SyncMessage[A]],
      syncAndValidatePipe: ConsensusService.SyncAndValidatePipe[F, A]#Right,
      consensusService: ConsensusService[F, A]
  ): Resource[F, SyncService[F, A]] =
    // TODO (PM-3187): Add Tracing
    // TODO (PM-3186): Add capacity as part of rate limiting.
    for {
      fiberPool <- FiberPool[F, A#PKey]()
      service = new SyncService[F, A](
        network,
        syncAndValidatePipe,
        consensusService,
        fiberPool
      )
      _ <- Concurrent[F].background(service.processMessages)
      _ <- Concurrent[F].background(service.processSyncAndValidateRequests)
    } yield service
}
