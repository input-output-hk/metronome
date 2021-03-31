package io.iohk.metronome.networking

import cats.effect.concurrent.{Deferred, TryableDeferred}
import cats.effect.implicits._
import cats.effect.{Concurrent, ContextShift, Resource, Sync}
import cats.implicits._
import io.iohk.metronome.networking.ConnectionHandler.HandledConnection.{
  HandledConnectionCloseReason,
  ManagerShutdown,
  RemoteClosed,
  RemoteError
}
import io.iohk.metronome.networking.ConnectionHandler.{
  ConnectionAlreadyClosedException,
  FinishedConnection,
  HandledConnection,
  MessageReceived
}
import io.iohk.metronome.networking.EncryptedConnectionProvider.{
  ConnectionAlreadyClosed,
  ConnectionError
}
import monix.catnap.ConcurrentQueue
import monix.execution.atomic.AtomicInt
import monix.tail.Iterant

import java.net.InetSocketAddress
import scala.util.control.NoStackTrace

class ConnectionHandler[F[_]: Concurrent, K, M](
    connectionQueue: ConcurrentQueue[
      F,
      (HandledConnection[F, K, M], Option[HandledConnection[F, K, M]])
    ],
    connectionsRegister: ConnectionsRegister[F, K, M],
    messageQueue: ConcurrentQueue[F, MessageReceived[K, M]],
    cancelToken: TryableDeferred[F, Unit],
    connectionFinishCallback: FinishedConnection[K] => F[Unit]
)(implicit tracers: NetworkTracers[F, K, M]) {

  private val numberOfRunningConnections = AtomicInt(0)

  private def incrementRunningConnections: F[Unit] = {
    Concurrent[F].delay(numberOfRunningConnections.increment())
  }

  private def decrementRunningConnections: F[Unit] = {
    Concurrent[F].delay(numberOfRunningConnections.decrement())
  }

  private def closeAndDeregisterConnection(
      handledConnection: HandledConnection[F, K, M]
  ): F[Unit] = {
    val close = for {
      _ <- decrementRunningConnections
      _ <- connectionsRegister.deregisterConnection(handledConnection)
      _ <- handledConnection.close
    } yield ()

    close.guarantee {
      tracers.deregistered(handledConnection)
    }
  }

  private def register(
      possibleNewConnection: HandledConnection[F, K, M]
  ): F[Unit] = {
    connectionsRegister.registerIfAbsent(possibleNewConnection).flatMap {
      case Some(oldConnection) =>
        // in case of conflict we let the downstream logic to take care of detailed handling of it
        connectionQueue.offer((possibleNewConnection, Some(oldConnection)))
      case None =>
        connectionQueue.offer((possibleNewConnection, None))
    }
  }

  /** Registers incoming connections and start handling incoming messages in background, in case connection is already handled
    * it closes it
    *
    * @param serverAddress, server address of incoming connection which should already be known
    * @param encryptedConnection, established connection
    */
  def registerIncoming(
      serverAddress: InetSocketAddress,
      encryptedConnection: EncryptedConnection[F, K, M]
  ): F[Unit] = {
    HandledConnection
      .incoming(cancelToken, serverAddress, encryptedConnection)
      .flatMap(connection => register(connection))

  }

  /** Registers out connections and start handling incoming messages in background, in case connection is already handled
    * it closes it
    *
    * @param encryptedConnection, established connection
    */
  def registerOutgoing(
      encryptedConnection: EncryptedConnection[F, K, M]
  ): F[Unit] = {
    HandledConnection
      .outgoing(cancelToken, encryptedConnection)
      .flatMap(connection => register(connection))
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
              // Closing the connection will cause it to be re-queued for reconnection.
              tracers.sendError(connection) >>
                connection.closeAlreadyClosed.as(
                  Left(ConnectionAlreadyClosedException(recipient))
                )

            case Right(_) =>
              tracers.sent((connection, message)).as(Right(()))
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
      case None =>
        connectionFinishCallback(
          FinishedConnection(
            handledConnection.key,
            handledConnection.serverAddress
          )
        )
    }
  }

  private def callCallBackWithConnection(
      handledConnection: HandledConnection[F, K, M]
  ): F[Unit] = {
    connectionFinishCallback(
      FinishedConnection(
        handledConnection.key,
        handledConnection.serverAddress
      )
    )
  }

  def handleConflict(
      newConnectionWithPossibleConflict: (
          HandledConnection[F, K, M],
          Option[HandledConnection[F, K, M]]
      )
  ): F[Option[HandledConnection[F, K, M]]] = {
    val (newConnection, possibleOldConnection) =
      newConnectionWithPossibleConflict

    possibleOldConnection match {
      case Some(value) =>
        newConnection.close.as(None: Option[HandledConnection[F, K, M]])
      case None =>
        Concurrent[F].pure(Some(newConnection))
    }
  }

  /** Connections multiplexer, it receives both incoming and outgoing connections and start reading incoming messages from
    * them concurrently, putting them on received messages queue.
    * In case of error or stream finish it cleans up all resources.
    */
  private def handleConnections: F[Unit] = {
    Iterant
      .repeatEvalF(connectionQueue.poll)
      .mapEval(handleConflict)
      .collect { case Some(newConnection) => newConnection }
      .mapEval { connection =>
        incrementRunningConnections >>
          Iterant
            .repeatEvalF(
              connection.incomingMessage
            )
            .takeWhile(_.isDefined)
            .map(_.get)
            .mapEval[Unit] { m =>
              tracers.received((connection, m)) >>
                messageQueue.offer(
                  MessageReceived(connection.key, m)
                )
            }
            .guarantee(
              // at this point closeReason will always be filled
              connection.closeReason.get.flatMap {
                case HandledConnection.RemoteClosed =>
                  closeAndDeregisterConnection(
                    connection
                  ) >> callCallBackWithConnection(connection)
                case RemoteError(e) =>
                  tracers.receiveError(
                    (connection, e)
                  ) >> closeAndDeregisterConnection(
                    connection
                  ) >> callCallBackWithConnection(connection)
                case HandledConnection.ManagerShutdown =>
                  closeAndDeregisterConnection(connection)
              }
            )
            .completedL
            .start
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
  sealed abstract case class HandledConnection[F[_]: Concurrent, K, M] private (
      globalCancelToken: TryableDeferred[F, Unit],
      key: K,
      serverAddress: InetSocketAddress,
      underlyingConnection: EncryptedConnection[F, K, M],
      deregisterListener: Deferred[F, Unit],
      closeReason: Deferred[F, HandledConnectionCloseReason]
  ) {
    def sendMessage(m: M): F[Unit] = {
      underlyingConnection.sendMessage(m)
    }

    def close: F[Unit] = {
      underlyingConnection.close
    }

    def closeAlreadyClosed: F[Unit] = {
      completeWithReason(RemoteClosed) >> underlyingConnection.close
    }

    private def completeWithReason(r: HandledConnectionCloseReason): F[Unit] =
      closeReason.complete(r).attempt.void

    private def handleIncomingEvent(
        incomingEvent: Option[Either[ConnectionError, M]]
    ): F[Option[M]] = {
      incomingEvent match {
        case Some(Right(m)) => Concurrent[F].pure(Some(m))
        case Some(Left(e))  => completeWithReason(RemoteError(e)).as(None)
        case None           => completeWithReason(RemoteClosed).as(None)
      }
    }

    def incomingMessage: F[Option[M]] = {
      Concurrent[F]
        .race(globalCancelToken.get, underlyingConnection.incomingMessage)
        .flatMap {
          case Left(_)  => completeWithReason(ManagerShutdown).as(None)
          case Right(e) => handleIncomingEvent(e)
        }
    }
  }

  object HandledConnection {
    sealed abstract class HandledConnectionCloseReason
    case object RemoteClosed extends HandledConnectionCloseReason
    case class RemoteError(e: ConnectionError)
        extends HandledConnectionCloseReason
    case object ManagerShutdown extends HandledConnectionCloseReason

    private def buildLifeCycleListeners[F[_]: Concurrent]
        : F[(Deferred[F, HandledConnectionCloseReason], Deferred[F, Unit])] = {
      for {
        closeReason        <- Deferred[F, HandledConnectionCloseReason]
        deregisterListener <- Deferred[F, Unit]
      } yield (closeReason, deregisterListener)
    }

    private[ConnectionHandler] def outgoing[F[_]: Concurrent, K, M](
        globalCancelToken: TryableDeferred[F, Unit],
        encryptedConnection: EncryptedConnection[F, K, M]
    ): F[HandledConnection[F, K, M]] = {
      buildLifeCycleListeners[F].map { case (closeReason, deregisterListener) =>
        new HandledConnection[F, K, M](
          globalCancelToken,
          encryptedConnection.remotePeerInfo._1,
          encryptedConnection.remotePeerInfo._2,
          encryptedConnection,
          deregisterListener,
          closeReason
        ) {}
      }
    }

    private[ConnectionHandler] def incoming[F[_]: Concurrent, K, M](
        globalCancelToken: TryableDeferred[F, Unit],
        serverAddress: InetSocketAddress,
        encryptedConnection: EncryptedConnection[F, K, M]
    ): F[HandledConnection[F, K, M]] = {
      buildLifeCycleListeners[F].map { case (closeReason, deregisterListener) =>
        new HandledConnection[F, K, M](
          globalCancelToken,
          encryptedConnection.remotePeerInfo._1,
          serverAddress,
          encryptedConnection,
          deregisterListener,
          closeReason
        ) {}
      }
    }

  }

  private def buildHandler[F[_]: Concurrent: ContextShift, K, M](
      connectionFinishCallback: FinishedConnection[K] => F[Unit]
  )(implicit
      tracers: NetworkTracers[F, K, M]
  ): F[ConnectionHandler[F, K, M]] = {
    for {
      cancelToken         <- Deferred.tryable[F, Unit]
      acquiredConnections <- ConnectionsRegister.empty[F, K, M]
      messageQueue        <- ConcurrentQueue.unbounded[F, MessageReceived[K, M]]()
      connectionQueue <- ConcurrentQueue
        .unbounded[
          F,
          (HandledConnection[F, K, M], Option[HandledConnection[F, K, M]])
        ]()
    } yield new ConnectionHandler[F, K, M](
      connectionQueue,
      acquiredConnections,
      messageQueue,
      cancelToken,
      connectionFinishCallback
    )
  }

  case class FinishedConnection[K](
      connectionKey: K,
      connectionServerAddress: InetSocketAddress
  )

  /** Starts connection handler, and polling form connections
    *
    * @param connectionFinishCallback, callback to be called when connection is finished and get de-registered
    */
  def apply[F[_]: Concurrent: ContextShift, K, M](
      connectionFinishCallback: FinishedConnection[K] => F[Unit]
  )(implicit
      tracers: NetworkTracers[F, K, M]
  ): Resource[F, ConnectionHandler[F, K, M]] = {
    Resource
      .make(buildHandler[F, K, M](connectionFinishCallback)) { handler =>
        handler.shutdown
      }
      .flatMap { handler =>
        for {
          _ <- handler.handleConnections.background
        } yield handler
      }
  }

}
