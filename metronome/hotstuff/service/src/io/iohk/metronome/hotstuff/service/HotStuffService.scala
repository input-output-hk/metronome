package io.iohk.metronome.hotstuff.service

import cats.effect.{Concurrent, ContextShift, Resource, Timer}
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
import io.iohk.metronome.hotstuff.service.pipes.SyncPipe

object HotStuffService {

  /** Start up the HotStuff service stack. */
  def apply[F[_]: Concurrent: ContextShift: Timer, A <: Agreement: Block](
      publicKey: A#PKey,
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

      syncPipe <- Resource.liftF { SyncPipe[F, A] }

      consensusService <- ConsensusService(
        publicKey,
        consensusNetwork,
        syncPipe.left,
        initState
      )

      syncService <- SyncService(
        syncNetwork,
        syncPipe.right,
        consensusService
      )
    } yield ()
}
