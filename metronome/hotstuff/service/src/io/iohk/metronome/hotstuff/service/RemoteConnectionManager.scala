package io.iohk.metronome.hotstuff.service

import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Concurrent, ContextShift, Resource, Sync}
import cats.implicits._
import cats.effect.implicits._
import io.iohk.metronome.hotstuff.service.RemoteConnectionManager.{
  Connection,
  ConnectionsRegister
}
import io.iohk.scalanet.peergroup.Channel
import io.iohk.scalanet.peergroup.PeerGroup.ServerEvent.ChannelCreated
import io.iohk.scalanet.peergroup.dynamictls.DynamicTLSPeerGroup.{
  FramingConfig,
  PeerInfo
}
import io.iohk.scalanet.peergroup.dynamictls.{DynamicTLSPeerGroup, Secp256k1}
import monix.catnap.ConcurrentQueue
import monix.eval.{Task, TaskLift}
import monix.execution.Scheduler
import monix.tail.Iterant
import org.bouncycastle.crypto.AsymmetricCipherKeyPair
import scodec.Codec
import scodec.bits.BitVector

import java.net.InetSocketAddress
import java.security.SecureRandom

/**
  */
class RemoteConnectionManager[F[_]: Sync: TaskLift, M: Codec](
    acquiredConnections: ConnectionsRegister[F, M],
    pg: DynamicTLSPeerGroup[M],
    concurrentQueue: ConcurrentQueue[F, (PeerInfo, M)]
) {

  def getLocalInfo: PeerInfo = pg.processAddress

  def getAcquiredConnections: F[Set[Connection[F, M]]] =
    acquiredConnections.getAllRegisteredConnections

  def incomingMessages: Iterant[F, (PeerInfo, M)] =
    Iterant.repeatEvalF(concurrentQueue.poll)

  def sendMessage(recipient: PeerInfo, message: M): F[Unit] = {
    acquiredConnections.getConnection(recipient.id).flatMap {
      case Some(connection) => connection.sendMessage(message)
      case None =>
        Sync[F].raiseError(
          new RuntimeException(s"Peer ${recipient}, already disconnected")
        )
    }
  }
}

object RemoteConnectionManager {

  case class Connection[F[_]: Concurrent: TaskLift, M: Codec](
      info: PeerInfo,
      channel: Channel[PeerInfo, M],
      channelRelease: F[Unit]
  ) {
    def sendMessage(m: M): F[Unit] = TaskLift[F].apply(channel.sendMessage(m))

  }

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

  case class ConnectionFailed(err: Throwable)

  def connectTo[F[_]: Concurrent: TaskLift, M: Codec](
      pg: DynamicTLSPeerGroup[M],
      peerInfo: PeerInfo
  ): F[Either[ConnectionFailed, Connection[F, M]]] = {
    TaskLift[F].apply(pg.client(peerInfo).allocated.attempt.flatMap {
      case Left(value) => Task(Left(ConnectionFailed(value)))
      case Right((channel, releaseToken)) =>
        Task(
          Right(Connection(peerInfo, channel, TaskLift[F].apply(releaseToken)))
        )
    })
  }

  def acquireConnections[F[_]: Concurrent: TaskLift, M: Codec](
      pg: DynamicTLSPeerGroup[M],
      semaphore: Semaphore[F],
      connectionsToAcquire: ConcurrentQueue[F, PeerInfo],
      connectionsRegister: ConnectionsRegister[F, M],
      connectionsQueue: ConcurrentQueue[F, Connection[F, M]]
  ): F[Unit] = {
    Iterant
      .repeatEvalF(connectionsToAcquire.poll)
      .mapEval { info =>
        connectTo(pg, info).flatMap {
          case Left(value) =>
            //TODO add logging and some smarter reconnection logic
            connectionsToAcquire.offer(info)
          case Right(connection) =>
            connectionsRegister
              .registerConnection(connection)
              .flatMap(_ => connectionsQueue.offer(connection))

        }
      }
      .completedL
  }

  def handleServerConnections[F[_]: Concurrent: TaskLift, M: Codec](
      pg: DynamicTLSPeerGroup[M],
      connectionsQueue: ConcurrentQueue[F, Connection[F, M]],
      connectionsRegister: ConnectionsRegister[F, M],
      semaphore: Semaphore[F]
  ): F[Unit] = {
    Iterant
      .repeatEvalF(TaskLift[F].apply(pg.nextServerEvent))
      .takeWhile(_.isDefined)
      .map(_.get)
      .collect(ChannelCreated.collector)
      .mapEval { case (channel, release) =>
        val connection =
          Connection(channel.to, channel, TaskLift[F].apply(release))
        connectionsRegister
          .registerConnection(connection)
          .flatMap(_ => connectionsQueue.offer(connection))
      }
      .completedL
  }

  class ConnectionsRegister[F[_]: Concurrent, M: Codec](
      register: Ref[F, Map[BitVector, Connection[F, M]]]
  ) {
    def registerConnection(connection: Connection[F, M]): F[Unit] = {
      register.update(current => current + (connection.info.id -> connection))
    }

    def deregisterConnection(connectionId: BitVector): F[Unit] = {
      register.update(current => current - connectionId)
    }

    def getAllRegisteredConnections: F[Set[Connection[F, M]]] = {
      register.get.map(m => m.values.toSet)
    }

    def getConnection(connectionId: BitVector): F[Option[Connection[F, M]]] =
      register.get.map(connections => connections.get(connectionId))

  }

  object ConnectionsRegister {
    def empty[F[_]: Concurrent, M: Codec]: F[ConnectionsRegister[F, M]] = {
      Ref
        .of(Map.empty[BitVector, Connection[F, M]])
        .map(ref => new ConnectionsRegister(ref))
    }
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
      semaphore           <- Resource.liftF(Semaphore(1))
      acquiredConnections <- Resource.liftF(ConnectionsRegister.empty)
      connectionsToAcquireQueue <- Resource.liftF(
        ConcurrentQueue.unbounded[F, PeerInfo]()
      )
      _ <- Resource.liftF(
        connectionsToAcquireQueue.offerMany(connectionsToAcquire)
      )
      connectionQueue <- Resource.liftF(
        ConcurrentQueue.unbounded[F, Connection[F, M]]()
      )
      messageQueue <- Resource.liftF(
        ConcurrentQueue.unbounded[F, (PeerInfo, M)]()
      )

      pg <- buildPeerGroup(
        bindAddress,
        nodeKeyPair,
        secureRandom,
        useNativeTlsImplementation,
        framingConfig,
        maxIncomingQueueSizePerPeer
      )
      _ <- acquireConnections(
        pg,
        semaphore,
        connectionsToAcquireQueue,
        acquiredConnections,
        connectionQueue
      ).background
      _ <- handleServerConnections(
        pg,
        connectionQueue,
        acquiredConnections,
        semaphore
      ).background
    } yield new RemoteConnectionManager[F, M](
      acquiredConnections,
      pg,
      messageQueue
    )

  }
}
