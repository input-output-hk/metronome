package io.iohk.metronome.hotstuff.service

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, Resource, Sync}
import cats.implicits._
import io.iohk.metronome.hotstuff.service.RemoteConnectionManager.Connection
import io.iohk.scalanet.peergroup.Channel
import io.iohk.scalanet.peergroup.dynamictls.DynamicTLSPeerGroup.{
  FramingConfig,
  PeerInfo
}
import io.iohk.scalanet.peergroup.dynamictls.{DynamicTLSPeerGroup, Secp256k1}
import monix.catnap.ConcurrentQueue
import monix.eval.TaskLift
import monix.execution.Scheduler
import monix.tail.Iterant
import org.bouncycastle.crypto.AsymmetricCipherKeyPair
import scodec.Codec
import scodec.bits.BitVector

import java.net.InetSocketAddress
import java.security.SecureRandom

class RemoteConnectionManager[F[_]: Sync: TaskLift, M: Codec](
    acquiredConnections: Ref[F, Map[BitVector, Connection[M]]],
    pg: DynamicTLSPeerGroup[M],
    concurrentQueue: ConcurrentQueue[F, (PeerInfo, M)]
) {

  def getLocalInfo: PeerInfo = pg.processAddress

  def getAcquiredConnections: F[Set[Connection[M]]] =
    acquiredConnections.get.map(m => m.values.toSet)

  def incomingMessages: Iterant[F, (PeerInfo, M)] =
    Iterant.repeatEvalF(concurrentQueue.poll)
  def sendMessage(recipient: PeerInfo, message: M): F[Unit] = Sync[F].delay(())
}

object RemoteConnectionManager {

  case class Connection[M](info: PeerInfo, channel: Channel[PeerInfo, M])

  def buildPeerGroup[F[_]: Concurrent: TaskLift, M: Codec](
      bindAddress: InetSocketAddress,
      nodeKeyPair: AsymmetricCipherKeyPair,
      secureRandom: SecureRandom,
      useNativeTlsImplementation: Boolean,
      framingConfig: FramingConfig,
      maxIncomingQueueSizePerPeer: Int
  )(implicit s: Scheduler) = {

    val config = DynamicTLSPeerGroup
      .Config(
        bindAddress,
        Secp256k1,
        nodeKeyPair,
        secureRandom,
        useNativeTlsImplementation,
        framingConfig,
        maxIncomingQueueSizePerPeer,
        None
      )
      .get

    DynamicTLSPeerGroup(config).mapK(TaskLift.apply)
  }

  def apply[F[_]: Concurrent: TaskLift, M: Codec](
      bindAddress: InetSocketAddress,
      nodeKeyPair: AsymmetricCipherKeyPair,
      secureRandom: SecureRandom,
      useNativeTlsImplementation: Boolean,
      framingConfig: FramingConfig,
      maxIncomingQueueSizePerPeer: Int,
      connectionsToAcquire: Set[PeerInfo]
  )(implicit
      s: Scheduler,
      cs: ContextShift[F]
  ): Resource[F, RemoteConnectionManager[F, M]] = {
    for {
      acquiredConnections <- Resource.liftF(
        Ref.of(Map.empty[BitVector, Connection[M]])
      )
      queue <- Resource.liftF(ConcurrentQueue.unbounded[F, (PeerInfo, M)]())
      pg <- buildPeerGroup(
        bindAddress,
        nodeKeyPair,
        secureRandom,
        useNativeTlsImplementation,
        framingConfig,
        maxIncomingQueueSizePerPeer
      )
    } yield new RemoteConnectionManager[F, M](acquiredConnections, pg, queue)

  }
}
