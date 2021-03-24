package io.iohk.metronome.networking

import cats.effect.{Concurrent, ContextShift, Resource}
import cats.effect.concurrent.Deferred
import io.iohk.metronome.networking.RemoteConnectionManager.{
  MessageReceived,
  UnexpectedConnectionError,
  withCancelToken
}
import monix.catnap.ConcurrentQueue
import monix.execution.atomic.AtomicInt
import monix.tail.Iterant
import cats.implicits._
import cats.effect.implicits._
import io.iohk.metronome.networking.ConnectionHandler.HandledConnection
import io.iohk.metronome.networking.EncryptedConnectionProvider.ConnectionError

import java.net.InetSocketAddress

class ConnectionHandler[F[_]: Concurrent, K, M](
    connectionQueue: ConcurrentQueue[F, HandledConnection[F, K, M]],
    connectionsRegister: ConnectionsRegister[F, K, M],
    messageQueue: ConcurrentQueue[F, MessageReceived[K, M]],
    cancelToken: Deferred[F, Unit],
    connectionFinishCallback: HandledConnection[F, K, M] => F[Unit]
) {

  private val numberOfRunningConnections = AtomicInt(0)

  private def closeAndDeregisterConnection(
      handledConnection: HandledConnection[F, K, M]
  ): F[Unit] = {
    for {
      _ <- Concurrent[F].delay(numberOfRunningConnections.decrement())
      _ <- handledConnection.close()
      _ <- connectionsRegister.deregisterConnection(handledConnection)
    } yield ()
  }

  def registerIfAbsent(
      possibleNewConnection: HandledConnection[F, K, M]
  ): F[Unit] = {
    connectionsRegister.registerIfAbsent(possibleNewConnection).flatMap {
      case Some(_) =>
        //TODO for now we are closing any new connections in case of conflict, we may investigate other strategies
        possibleNewConnection.close()
      case None =>
        connectionQueue.offer(possibleNewConnection)
    }
  }

  def isNewConnection(connectionKey: K): F[Boolean] = {
    connectionsRegister.isNewConnection(connectionKey)
  }

  def getAllActiveConnections: F[Set[K]] =
    connectionsRegister.getAllRegisteredConnections.map { connections =>
      connections.map(_.key)
    }

  def numberOfActiveConnections: F[Int] = {
    Concurrent[F].delay(numberOfRunningConnections.get())
  }

  def incomingMessages: Iterant[F, MessageReceived[K, M]] =
    Iterant.repeatEvalF(messageQueue.poll)

  def getConnection(key: K): F[Option[HandledConnection[F, K, M]]] =
    connectionsRegister.getConnection(key)

  /** Connections multiplexer, it receives both incoming and outgoing connections and start reading incoming messages from
    * them concurrently, putting them on received messages queue.
    * In case of error or stream finish it cleans up all resources.
    */
  private def handleConnections: F[Unit] = {
    Iterant
      .repeatEvalF(connectionQueue.poll)
      .mapEval { connection =>
        Concurrent[F].delay(numberOfRunningConnections.increment()).flatMap {
          _ =>
            Iterant
              .repeatEvalF(
                withCancelToken(cancelToken, connection.incomingMessage)
              )
              .takeWhile(_.isDefined)
              .map(_.get)
              .mapEval {
                case Right(m) =>
                  messageQueue.offer(
                    MessageReceived(connection.key, m)
                  )
                case Left(e) =>
                  Concurrent[F].raiseError[Unit](
                    UnexpectedConnectionError(e, connection.key)
                  )
              }
              .guarantee(
                closeAndDeregisterConnection(connection)
                  .flatMap(_ => connectionFinishCallback(connection))
              )
              .completedL
              .start
        }
      }
      .completedL
  }

  private def shutdown: F[Unit] = cancelToken.complete(()).attempt.void
}

object ConnectionHandler {
  case class HandledConnection[F[_], K, M](
      key: K,
      serverAddress: InetSocketAddress,
      underlyingConnection: EncryptedConnection[F, K, M]
  ) {
    def sendMessage(m: M): F[Unit] = {
      underlyingConnection.sendMessage(m)
    }

    def close(): F[Unit] = {
      underlyingConnection.close()
    }

    def incomingMessage: F[Option[Either[ConnectionError, M]]] = {
      underlyingConnection.incomingMessage
    }
  }

  object HandledConnection {
    def outgoing[F[_], K, M](
        encryptedConnection: EncryptedConnection[F, K, M]
    ): HandledConnection[F, K, M] = {
      HandledConnection(
        encryptedConnection.remotePeerInfo._1,
        encryptedConnection.remotePeerInfo._2,
        encryptedConnection
      )
    }

    def incoming[F[_], K, M](
        serverAddress: InetSocketAddress,
        encryptedConnection: EncryptedConnection[F, K, M]
    ): HandledConnection[F, K, M] = {
      HandledConnection(
        encryptedConnection.remotePeerInfo._1,
        serverAddress,
        encryptedConnection
      )
    }

  }

  def apply[F[_]: Concurrent: ContextShift, K, M](
      connectionFinishCallback: HandledConnection[F, K, M] => F[Unit]
  ): F[ConnectionHandler[F, K, M]] = {
    for {
      cancelToken         <- Deferred[F, Unit]
      acquiredConnections <- ConnectionsRegister.empty[F, K, M]
      messageQueue        <- ConcurrentQueue.unbounded[F, MessageReceived[K, M]]()
      connectionQueue <- ConcurrentQueue
        .unbounded[F, HandledConnection[F, K, M]]()
    } yield new ConnectionHandler[F, K, M](
      connectionQueue,
      acquiredConnections,
      messageQueue,
      cancelToken,
      connectionFinishCallback
    )
  }

  def connectionHandlerResource[F[_]: Concurrent: ContextShift, K, M](
      connectionFinishCallback: HandledConnection[F, K, M] => F[Unit]
  ): Resource[F, ConnectionHandler[F, K, M]] = {
    Resource
      .make(ConnectionHandler(connectionFinishCallback)) { handler =>
        handler.shutdown
      }
      .flatMap { handler =>
        for {
          _ <- handler.handleConnections.background
        } yield handler
      }
  }

}
