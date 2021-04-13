package io.iohk.metronome.hotstuff.service

import cats.implicits._
import cats.effect.{Sync, Resource, Concurrent}
import io.iohk.metronome.core.FiberPool
import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.hotstuff.service.messages.SyncMessage
import io.iohk.metronome.networking.ConnectionHandler
import cats.effect.ContextShift

class SyncService[F[_]: Sync, A <: Agreement](
    network: Network[F, A, SyncMessage[A]],
    consensusService: ConsensusService[F, A],
    fiberPool: FiberPool[F, A#PKey]
) {

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
                case None => ().pure[F]
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
}

object SyncService {

  /** Create a `SyncService` instance and start processing messages
    * in the background, shutting processing down when the resource is
    * released.
    */
  def apply[F[_]: Concurrent: ContextShift, A <: Agreement](
      network: Network[F, A, SyncMessage[A]],
      consensusService: ConsensusService[F, A]
  ): Resource[F, SyncService[F, A]] =
    // TODO (PM-3187): Add Tracing
    // TODO (PM-3186): Add capacity as part of rate limiting.
    for {
      fiberPool <- FiberPool[F, A#PKey]()
      service = new SyncService[F, A](network, consensusService, fiberPool)
      _ <- Concurrent[F].background(service.processMessages)
    } yield service
}
