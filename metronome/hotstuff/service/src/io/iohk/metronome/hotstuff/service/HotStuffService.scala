package io.iohk.metronome.hotstuff.service

import cats.effect.{Concurrent, ContextShift, Resource, Timer}
import io.iohk.metronome.core.Pipe
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  ProtocolState,
  Message,
  Block
}
import io.iohk.metronome.hotstuff.service.messages.{
  HotStuffMessage,
  SyncMessage
}

object HotStuffService {
  import ConsensusService.{SyncAndValidateRequest, SyncAndValidateResponse}

  /** Start up the HotStuff service stack. */
  def apply[F[_]: Concurrent: ContextShift: Timer, A <: Agreement: Block](
      network: Network[F, A, HotStuffMessage[A]],
      initState: ProtocolState[A]
  ): Resource[F, Unit] =
    for {
      (consensusNetwork, syncNetwork) <- Network
        .splitter[F, A, HotStuffMessage[A], Message[A], SyncMessage[A]](
          network
        )(
          split = {
            case HotStuffMessage.ConsensusMessage(message) => Left(message)
            case HotStuffMessage.SyncMessage(message)      => Right(message)
          },
          merge = {
            case Left(message)  => HotStuffMessage.ConsensusMessage(message)
            case Right(message) => HotStuffMessage.SyncMessage(message)
          }
        )
      syncAndValidatePipe <- Resource.liftF {
        Pipe[F, SyncAndValidateRequest[A], SyncAndValidateResponse[A]]
      }
      consensusService <- ConsensusService(
        consensusNetwork,
        syncAndValidatePipe.left,
        initState
      )
      syncService <- SyncService(
        syncNetwork,
        syncAndValidatePipe.right,
        consensusService
      )
    } yield ()
}
