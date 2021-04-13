package io.iohk.metronome.hotstuff.service

import cats.implicits._
import cats.effect.{Sync, Resource, Concurrent}
import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.hotstuff.service.messages.SyncMessage
import io.iohk.metronome.networking.ConnectionHandler

class SyncService[F[_]: Sync, A <: Agreement](
    network: Network[F, A, SyncMessage[A]],
    consensusService: ConsensusService[F, A]
) {

  /** Process incoming network messages */
  private def processMessages: F[Unit] = {
    import SyncMessage._
    // TODO (PM-3186): Rate limiting per source.
    network.incomingMessages
      .mapEval[Unit] { case ConnectionHandler.MessageReceived(from, message) =>
        message match {
          case GetStatusRequest(requestId) =>
            consensusService.getState.flatMap { state =>
              val status =
                Status(state.viewNumber, state.prepareQC, state.commitQC)
              network.sendMessage(from, GetStatusResponse(requestId, status))
            }

          case GetBlockRequest(requestId, blockHash) =>
            // TODO (PM-3134): Retreive block from storage and respond.
            val getBlock: F[Option[A#Block]] = ???

            getBlock.flatMap {
              case None => ().pure[F]
              case Some(block) =>
                network.sendMessage(from, GetBlockResponse(requestId, block))
            }

          case GetStatusResponse(requestId, status) =>
            // TODO (PM-3063): Hand over to view synchronisation.
            ???

          case GetBlockResponse(requestId, block) =>
            // TODO (PM-3134): Hand over to block synchronisation.
            ???
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
  def apply[F[_]: Concurrent, A <: Agreement](
      network: Network[F, A, SyncMessage[A]],
      consensusService: ConsensusService[F, A]
  ): Resource[F, SyncService[F, A]] =
    // TODO (PM-3187): Add Tracing
    for {
      service <- Resource.liftF {
        Sync[F].delay {
          new SyncService[F, A](network, consensusService)
        }
      }
      _ <- Concurrent[F].background(service.processMessages)
    } yield service
}
