package io.iohk.metronome.hotstuff.service

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.implicits._
import cats.effect.{Concurrent, ContextShift, Resource, Sync}
import cats.implicits._
import io.iohk.metronome.hotstuff.service.RemoteConnectionManager.{
  ConnectionsRegister,
  MessageReceived
}
import monix.catnap.ConcurrentQueue
import monix.eval.TaskLift
import monix.execution.Scheduler
import monix.tail.Iterant
import scodec.Codec
import scodec.bits.BitVector

import java.net.InetSocketAddress

/**
  */
class RemoteConnectionManager[F[_]: Sync: TaskLift, K: Codec, M: Codec](
    acquiredConnections: ConnectionsRegister[F, K, M],
    localInfo: (K, InetSocketAddress),
    concurrentQueue: ConcurrentQueue[F, MessageReceived[K, M]]
) {

  def getLocalInfo: (K, InetSocketAddress) = localInfo

  def getAcquiredConnections: F[Set[EncryptedConnection[F, K, M]]] =
    acquiredConnections.getAllRegisteredConnections

  def incomingMessages: Iterant[F, MessageReceived[K, M]] =
    Iterant.repeatEvalF(concurrentQueue.poll)

  def sendMessage(recipient: BitVector, message: M): F[Unit] = {
    acquiredConnections.getConnection(recipient).flatMap {
      case Some(connection) =>
        connection.sendMessage(message)
      case None =>
        Sync[F].raiseError(
          new RuntimeException(s"Peer ${recipient}, already disconnected")
        )
    }
  }
}

object RemoteConnectionManager {

  case class MessageReceived[K, M](from: K, message: M)

  def acquireConnections[F[_]: Concurrent: TaskLift, K: Codec, M: Codec](
      encryptedConnectionProvider: EncryptedConnectionProvider[F, K, M],
      connectionsToAcquire: ConcurrentQueue[F, OutGoingConnectionRequest[K]],
      connectionsRegister: ConnectionsRegister[F, K, M],
      connectionsQueue: ConcurrentQueue[F, EncryptedConnection[F, K, M]]
  ): F[Unit] = {
    Iterant
      .repeatEvalF(connectionsToAcquire.poll)
      .mapEval { connectionRequest =>
        encryptedConnectionProvider
          .connectTo(connectionRequest.key, connectionRequest.address)
          .redeemWith(
            //TODO add logging and some smarter reconnection logic
            e => connectionsToAcquire.offer(connectionRequest),
            connection =>
              connectionsRegister
                .registerConnection(connection)
                .flatMap(_ => connectionsQueue.offer(connection))
          )
      }
      .completedL
  }

  def handleServerConnections[F[_]: Concurrent: TaskLift, K, M: Codec](
      pg: EncryptedConnectionProvider[F, K, M],
      connectionsQueue: ConcurrentQueue[F, EncryptedConnection[F, K, M]],
      connectionsRegister: ConnectionsRegister[F, K, M]
  ): F[Unit] = {
    Iterant
      .repeatEvalF(pg.incomingConnection)
      .takeWhile(_.isDefined)
      .map(_.get)
      .collect { case Right(value) =>
        value
      }
      .mapEval { encryptedConnection =>
        connectionsRegister
          .registerConnection(encryptedConnection)
          .flatMap(_ => connectionsQueue.offer(encryptedConnection))
      }
      .completedL
  }

  def withCancelToken[F[_]: Concurrent, A](
      token: Deferred[F, Unit],
      ops: F[Option[A]]
  ): F[Option[A]] =
    Concurrent[F].race(token.get, ops).map {
      case Left(()) => None
      case Right(x) => x
    }

  def handleConnections[F[_]: Concurrent: TaskLift, K: Codec, M: Codec](
      q: ConcurrentQueue[F, EncryptedConnection[F, K, M]],
      connectionsRegister: ConnectionsRegister[F, K, M],
      messageQueue: ConcurrentQueue[F, MessageReceived[K, M]]
  ): F[Unit] = {
    Deferred[F, Unit].flatMap { cancelToken =>
      Iterant
        .repeatEvalF(q.poll)
        .mapEval { connection =>
          Iterant
            .repeatEvalF(
              withCancelToken(cancelToken, connection.incomingMessage)
            )
            .takeWhile(_.isDefined)
            .map(_.get)
            .mapEval {
              case Right(m) =>
                messageQueue.offer(
                  MessageReceived(connection.remotePeerInfo._1, m)
                )
              case Left(e) =>
                Concurrent[F].raiseError[Unit](
                  new RuntimeException("Unexpected Error")
                )
            }
            .guarantee(
              connectionsRegister.deregisterConnection(connection)
            )
            .completedL
            .start
        }
        .completedL
        .guarantee(cancelToken.complete(()))
    }
  }

  class ConnectionsRegister[F[_]: Concurrent, K: Codec, M: Codec](
      register: Ref[F, Map[BitVector, EncryptedConnection[F, K, M]]]
  ) {

    def registerConnection(
        connection: EncryptedConnection[F, K, M]
    ): F[Unit] = {
      register.update(current =>
        current + (connection.getSerializedKey -> connection)
      )
    }

    def deregisterConnection(
        connection: EncryptedConnection[F, K, M]
    ): F[Unit] = {
      register.update(current => current - (connection.getSerializedKey))
    }

    def getAllRegisteredConnections: F[Set[EncryptedConnection[F, K, M]]] = {
      register.get.map(m => m.values.toSet)
    }

    def getConnection(
        connectionId: BitVector
    ): F[Option[EncryptedConnection[F, K, M]]] =
      register.get.map(connections => connections.get(connectionId))

  }

  object ConnectionsRegister {
    def empty[F[_]: Concurrent, K: Codec, M: Codec]
        : F[ConnectionsRegister[F, K, M]] = {
      Ref
        .of(Map.empty[BitVector, EncryptedConnection[F, K, M]])
        .map(ref => new ConnectionsRegister[F, K, M](ref))
    }
  }

  case class OutGoingConnectionRequest[K](key: K, address: InetSocketAddress)

  def apply[F[_]: Concurrent: TaskLift, K: Codec, M: Codec](
      encryptedConnectionsProvider: EncryptedConnectionProvider[F, K, M],
      connectionsToAcquire: Set[OutGoingConnectionRequest[K]]
  )(implicit
      s: Scheduler,
      cs: ContextShift[F]
  ): Resource[F, RemoteConnectionManager[F, K, M]] = {
    for {
      acquiredConnections <- Resource.liftF(ConnectionsRegister.empty[F, K, M])
      connectionsToAcquireQueue <- Resource.liftF(
        ConcurrentQueue.unbounded[F, OutGoingConnectionRequest[K]]()
      )
      _ <- Resource.liftF(
        connectionsToAcquireQueue.offerMany(connectionsToAcquire)
      )
      connectionQueue <- Resource.liftF(
        ConcurrentQueue.unbounded[F, EncryptedConnection[F, K, M]]()
      )
      messageQueue <- Resource.liftF(
        ConcurrentQueue.unbounded[F, MessageReceived[K, M]]()
      )

      _ <- acquireConnections(
        encryptedConnectionsProvider,
        connectionsToAcquireQueue,
        acquiredConnections,
        connectionQueue
      ).background
      _ <- handleServerConnections(
        encryptedConnectionsProvider,
        connectionQueue,
        acquiredConnections
      ).background

      _ <- handleConnections(
        connectionQueue,
        acquiredConnections,
        messageQueue
      ).background
    } yield new RemoteConnectionManager[F, K, M](
      acquiredConnections,
      encryptedConnectionsProvider.localInfo,
      messageQueue
    )

  }
}
