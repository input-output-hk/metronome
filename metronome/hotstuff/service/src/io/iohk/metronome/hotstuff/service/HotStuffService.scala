package io.iohk.metronome.hotstuff.service

import cats.Parallel
import cats.effect.{Concurrent, ContextShift, Resource, Timer}
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  ProtocolState,
  Message,
  Block,
  Signing
}
import io.iohk.metronome.hotstuff.service.messages.{
  HotStuffMessage,
  SyncMessage
}
import io.iohk.metronome.hotstuff.service.pipes.SyncPipe
import io.iohk.metronome.hotstuff.service.storage.{
  BlockStorage,
  ViewStateStorage
}
import io.iohk.metronome.hotstuff.service.tracing.{
  ConsensusTracers,
  SyncTracers
}
import io.iohk.metronome.storage.KVStoreRunner

object HotStuffService {

  /** Start up the HotStuff service stack. */
  def apply[F[
      _
  ]: Concurrent: ContextShift: Timer: Parallel, N, A <: Agreement: Block: Signing](
      network: Network[F, A, HotStuffMessage[A]],
      appService: ApplicationService[F, A],
      blockStorage: BlockStorage[N, A],
      viewStateStorage: ViewStateStorage[N, A],
      initState: ProtocolState[A]
  )(implicit
      consensusTracers: ConsensusTracers[F, A],
      syncTracers: SyncTracers[F, A],
      storeRunner: KVStoreRunner[F, N]
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
        initState.publicKey,
        consensusNetwork,
        appService,
        blockStorage,
        viewStateStorage,
        syncPipe.left,
        initState
      )

      syncService <- SyncService(
        initState.publicKey,
        initState.federation,
        syncNetwork,
        appService,
        blockStorage,
        viewStateStorage,
        syncPipe.right,
        consensusService.getState
      )
    } yield ()
}
