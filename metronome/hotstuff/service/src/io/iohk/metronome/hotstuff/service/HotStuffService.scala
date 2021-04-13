package io.iohk.metronome.hotstuff.service

import cats.effect.{Concurrent, ContextShift, Resource, Timer}
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  ProtocolState,
  Message
}
import io.iohk.metronome.hotstuff.service.messages.{
  HotStuffMessage,
  SyncMessage
}
import io.iohk.metronome.hotstuff.service.messages.SyncMessage

object HotStuffService {
  def apply[F[_]: Concurrent: ContextShift: Timer, A <: Agreement](
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
      consensusService <- ConsensusService(consensusNetwork, initState)
    } yield ()
}
