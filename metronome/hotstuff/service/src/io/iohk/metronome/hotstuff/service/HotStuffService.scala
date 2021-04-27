package io.iohk.metronome.hotstuff.service

import cats.Parallel
import cats.effect.{Concurrent, ContextShift, Resource, Timer}
import io.iohk.metronome.hotstuff.consensus.Federation
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
import io.iohk.metronome.hotstuff.service.pipes.BlockSyncPipe
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
      publicKey: A#PKey,
      federation: Federation[A#PKey],
      network: Network[F, A, HotStuffMessage[A]],
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

      blockSyncPipe <- Resource.liftF { BlockSyncPipe[F, A] }

      consensusService <- ConsensusService(
        publicKey,
        consensusNetwork,
        blockStorage,
        viewStateStorage,
        blockSyncPipe.left,
        initState
      )

      syncService <- SyncService(
        publicKey,
        federation,
        syncNetwork,
        blockStorage,
        blockSyncPipe.right,
        consensusService.getState
      )
    } yield ()
}
