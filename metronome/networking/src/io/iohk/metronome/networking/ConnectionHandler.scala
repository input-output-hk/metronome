package io.iohk.metronome.networking

import cats.effect.{Concurrent, ContextShift, Resource, Sync}
import cats.effect.concurrent.{Deferred, TryableDeferred}
import io.iohk.metronome.networking.RemoteConnectionManager.withCancelToken
import monix.catnap.ConcurrentQueue
import monix.execution.atomic.AtomicInt
import monix.tail.Iterant
import cats.implicits._
import cats.effect.implicits._
import io.iohk.metronome.networking.ConnectionHandler.{
  ConnectionAlreadyClosedException,
  HandledConnection,
  MessageReceived,
  UnexpectedConnectionError
}
import io.iohk.metronome.networking.EncryptedConnectionProvider.{
  ConnectionAlreadyClosed,
  ConnectionError
}

import java.net.InetSocketAddress
import scala.util.control.NoStackTrace

class ConnectionHandler[F[_]: Concurrent, K, M](
    connectionQueue: ConcurrentQueue[F, HandledConnection[F, K, M]],
    connectionsRegister: ConnectionsRegister[F, K, M],
    messageQueue: ConcurrentQueue[F, MessageReceived[K, M]],
    cancelToken: TryableDeferred[F, Unit],
    connectionFinishCallback: HandledConnection[F, K, M] => F[Unit]
) {

  private val numberOfRunningConnections = AtomicInt(0)

  private def closeAndDeregisterConnection(
      handledConnection: HandledConnection[F, K, M]
  ): F[Unit] = {
    for {
      _ <- Concurrent[F].delay(numberOfRunningConnections.decrement())
      _ <- connectionsRegister.deregisterConnection(handledConnection)
      _ <- handledConnection.close
    } yield ()
  }

  /** Registers connections and start handling incoming messages in background, in case connection is already handled
    * it closes it
    *
    * @param possibleNewConnection, possible connection to handle
    */
  def registerOrClose(
      possibleNewConnection: HandledConnection[F, K, M]
  ): F[Unit] = {
    connectionsRegister.registerIfAbsent(possibleNewConnection).flatMap {
      case Some(_) =>
        //TODO [PM-3092] for now we are closing any new connections in case of conflict, we may investigate other strategies
        // like keeping old for outgoing and replacing for incoming
        possibleNewConnection.close
      case None =>
        connectionQueue.offer(possibleNewConnection)
    }
  }

  /** Checks if handler already handles connection o peer with provided key
    *
    * @param connectionKey key of remote peer
    */
  def isNewConnection(connectionKey: K): F[Boolean] = {
    connectionsRegister.isNewConnection(connectionKey)
  }

  /** Retrieves set of keys of all connected and handled peers
    */
  def getAllActiveConnections: F[Set[K]] =
    connectionsRegister.getAllRegisteredConnections.map { connections =>
      connections.map(_.key)
    }

  /** Number of connections actively red in background
    */
  def numberOfActiveConnections: F[Int] = {
    Concurrent[F].delay(numberOfRunningConnections.get())
  }

  /** Stream of all messages received from all remote peers
    */
  def incomingMessages: Iterant[F, MessageReceived[K, M]] =
    Iterant.repeatEvalF(messageQueue.poll)

  /** Retrieves handled connection if one exists
    *
    * @param key, key of remote peer
    */
  def getConnection(key: K): F[Option[HandledConnection[F, K, M]]] =
    connectionsRegister.getConnection(key)

  def sendMessage(
      recipient: K,
      message: M
  ): F[Either[ConnectionAlreadyClosedException[K], Unit]] = {
    getConnection(recipient).flatMap {
      case Some(connection) =>
        connection
          .sendMessage(message)
          .attemptNarrow[ConnectionAlreadyClosed]
          .flatMap {
            case Left(_) =>
              connection.close.map { _ =>
                Left(ConnectionAlreadyClosedException(recipient))
              }

            case Right(value) =>
              Concurrent[F].pure(Right(()))
          }
      case None =>
        Concurrent[F].pure(Left(ConnectionAlreadyClosedException(recipient)))
    }
  }

  private def callCallBackIfNotClosed(
      handledConnection: HandledConnection[F, K, M]
  ): F[Unit] = {
    cancelToken.tryGet.flatMap {
      case Some(_) => Sync[F].unit
      case None    => connectionFinishCallback(handledConnection)
    }
  }

  /** Connections multiplexer, it receives both incoming and outgoing connections and start reading incoming messages from
    * them concurrently, putting them on received messages queue.
    * In case of error or stream finish it cleans up all resources.
    */
  private def handleConnections: F[Unit] = {
    Iterant
      .repeatEvalF(connectionQueue.poll)
      .mapEval { connection =>
        Sync[F].delay(numberOfRunningConnections.increment()).flatMap { _ =>
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
                .flatMap(_ => callCallBackIfNotClosed(connection))
            )
            .completedL
            .start
        }
      }
      .completedL
  }

  // for now shutdown of all connections is completed in background
  private def shutdown: F[Unit] = cancelToken.complete(()).attempt.void
}

object ConnectionHandler {
  case class ConnectionAlreadyClosedException[K](key: K)
      extends RuntimeException(
        s"Connection with node ${key}, has already closed"
      )
      with NoStackTrace

  private def getConnectionErrorMessage[K](
      e: ConnectionError,
      connectionKey: K
  ): String = {
    e match {
      case EncryptedConnectionProvider.DecodingError =>
        s"Unexpected decoding error on connection with ${connectionKey}"
      case EncryptedConnectionProvider.UnexpectedError(ex) =>
        s"Unexpected error ${ex.getMessage} on connection with ${connectionKey}"
    }
  }

  case class UnexpectedConnectionError[K](e: ConnectionError, connectionKey: K)
      extends RuntimeException(getConnectionErrorMessage(e, connectionKey))

  case class MessageReceived[K, M](from: K, message: M)

  /** Connection which is already handled by connection handler i.e it is registered in registry and handler is subscribed
    * for incoming messages of that connection
    *
    * @param key, key of remote node
    * @param serverAddress, address of the server of remote node. In case of incoming connection it will be diffrent that
    *                       underlyingConnection remoteAddress
    * @param underlyingConnection, encrypted connection to send and receive messages
    */
  case class HandledConnection[F[_], K, M](
      key: K,
      serverAddress: InetSocketAddress,
      underlyingConnection: EncryptedConnection[F, K, M]
  ) {
    def sendMessage(m: M): F[Unit] = {
      underlyingConnection.sendMessage(m)
    }

    def close: F[Unit] = {
      underlyingConnection.close
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

  private def buildHandler[F[_]: Concurrent: ContextShift, K, M](
      connectionFinishCallback: HandledConnection[F, K, M] => F[Unit]
  ): F[ConnectionHandler[F, K, M]] = {
    for {
      cancelToken         <- Deferred.tryable[F, Unit]
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

  def apply[F[_]: Concurrent: ContextShift, K, M](
      connectionFinishCallback: HandledConnection[F, K, M] => F[Unit]
  ): Resource[F, ConnectionHandler[F, K, M]] = {
    Resource
      .make(buildHandler(connectionFinishCallback)) { handler =>
        handler.shutdown
      }
      .flatMap { handler =>
        for {
          _ <- handler.handleConnections.background
        } yield handler
      }
  }

}
