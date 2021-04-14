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
import io.iohk.metronome.hotstuff.consensus.basic.QuorumCertificate

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

      consensusStorage = makeConsensusStorage[F, A]

      consensusService <- ConsensusService(
        publicKey,
        consensusNetwork,
        consensusStorage,
        syncPipe.left,
        initState
      )

      syncService <- SyncService(
        syncNetwork,
        syncPipe.right,
        consensusService
      )
    } yield ()

  /** Construct the storage component for conensus from BlockStorage and
    * a query compiler that turns `KVStore[N, R]` to `F[R]`.
    */
  private def makeConsensusStorage[F[_], A <: Agreement]
      : ConsensusService.Storage[F, A] =
    new ConsensusService.Storage[F, A] {
      // TODO (PM-3104): Persist Block
      override def saveBlock(block: A#Block): F[Unit] = ???

      // TODO (PM-3112): Persist View State.
      override def saveCommitQC(qc: QuorumCertificate[A]): F[Unit] = ???
    }

}
