package io.iohk.metronome.networking

import cats.effect.concurrent.Deferred
import cats.effect.implicits._
import cats.effect.{Concurrent, ContextShift, Resource, Sync}
import cats.implicits._
import io.iohk.metronome.networking.ConnectionHandler.HandledConnection._
import io.iohk.metronome.networking.ConnectionHandler.{
  ConnectionAlreadyClosedException,
  ConnectionWithConflictFlag,
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
import scala.concurrent.duration._
import java.time.Instant

class ConnectionHandler[F[_]: Concurrent, K, M](
    connectionQueue: ConcurrentQueue[F, ConnectionWithConflictFlag[F, K, M]],
    connectionsRegister: ConnectionsRegister[F, K, M],
    messageQueue: ConcurrentQueue[F, MessageReceived[K, M]],
    cancelToken: Deferred[F, Unit],
    connectionFinishCallback: FinishedConnection[K] => F[Unit],
    oppositeConnectionOverlap: FiniteDuration
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
      maybeConflicting =>
        // in case of conflict we deal with it in the background
        connectionQueue.offer(
          (possibleNewConnection, maybeConflicting.isDefined)
        )
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

  /** Send message to remote peer if its connected
    *
    * @param recipient, key of the remote peer
    * @param message message to send
    */
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

  def receiveMessage(
      sender: K,
      message: M
  ): F[Unit] =
    messageQueue.offer(MessageReceived(sender, message))

  private def handleConflict(
      newConnectionWithPossibleConflict: ConnectionWithConflictFlag[F, K, M]
  ): F[Option[HandledConnection[F, K, M]]] = {
    val (newConnection, conflictHappened) =
      newConnectionWithPossibleConflict

    if (conflictHappened) {
      connectionsRegister.registerIfAbsent(newConnection).flatMap {
        case Some(oldConnection) =>
          val replace = shouldReplaceConnection(
            newConnection = newConnection,
            oldConnection = oldConnection
          )
          if (replace) {
            replaceConnection(newConnection, oldConnection)
          } else {
            tracers.discarded(newConnection) >> newConnection.close.as(none)
          }
        case None =>
          // in the meantime between detection of conflict, and processing it old connection has dropped. Register new one
          tracers.registered(newConnection) >> newConnection.some.pure[F]
      }
    } else {
      tracers.registered(newConnection) >> newConnection.some.pure[F]
    }
  }

  /** Decide whether a new connection to/from a peer should replace an old connection from/to the same peer in case of a conflict. */
  private def shouldReplaceConnection(
      newConnection: HandledConnection[F, K, M],
      oldConnection: HandledConnection[F, K, M]
  ): Boolean = {
    if (oldConnection.age() < oppositeConnectionOverlap) {
      // The old connection has just been created recently, yet we have a new connection already.
      // Most likely the two nodes opened connections to each other around the same time, and if
      // we close one of the connections connection based on direction, the node opposite will
      // likely be doing  the same to the _other_ connection, symmetrically.
      // Instead, let's try to establish some ordering between the two, so the same connection
      // is chosen as the victim on both sides.
      val (newPort, oldPort) = (
        newConnection.ephemeralAddress.getPort,
        oldConnection.ephemeralAddress.getPort
      )
      newPort < oldPort || newPort == oldPort &&
      newConnection.ephemeralAddress.getHostName < oldConnection.ephemeralAddress.getHostName
    } else {
      newConnection.connectionDirection match {
        case HandledConnection.IncomingConnection =>
          // Even though we have connection to this peer, they are calling us. One of the reason may be
          // that they failed and we did not notice. Lets try to replace old connection with new one.
          true

        case HandledConnection.OutgoingConnection =>
          // For some reason we were calling while we already have connection, most probably we have
          // received incoming connection during call. Close this new connection, and keep the old one.
          false
      }
    }
  }

  /** Safely replaces old connection from remote peer with new connection with same remote peer.
    *
    * 1. The callback for old connection will not be called. As from the perspective of outside world connection is never
    *    finished
    * 2. From the point of view of outside world connection never leaves connection registry i.e during replacing all call to
    *    registerOutgoing or registerIncoming will report conflicts to be handled
    */
  private def replaceConnection(
      newConnection: HandledConnection[F, K, M],
      oldConnection: HandledConnection[F, K, M]
  ): F[Option[HandledConnection[F, K, M]]] = {
    for {
      result <- oldConnection.requestReplace(newConnection)
      maybeNew <- result match {
        case ConnectionHandler.ReplaceFinished =>
          // Replace succeeded, old connection should already be closed and discarded, pass the new one forward
          tracers.registered(newConnection) >>
            newConnection.some.pure[F]
        case ConnectionHandler.ConnectionAlreadyDisconnected =>
          // during or just before replace, old connection disconnected for some other reason,
          // the reconnect call back will be fired either way so close the new connection
          tracers.discarded(newConnection) >>
            newConnection.close.as(None: Option[HandledConnection[F, K, M]])
      }

    } yield maybeNew
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

  private def handleReplace(
      replaceRequest: ReplaceRequested[F, K, M]
  ): F[Unit] = {
    connectionsRegister.replace(replaceRequest.newConnection).flatMap {
      case Some(oldConnection) =>
        // close connection just in case someone who requested replace forgot it
        oldConnection.close
      case None =>
        // this case should not happen, as we handle each connection in separate fiber, and only this fiber can remove
        // connection with given key.
        ().pure[F]
    } >> replaceRequest.signalReplaceSuccess
  }

  private def handleConnectionFinish(
      connection: HandledConnection[F, K, M]
  ): F[Unit] = {
    // at this point closeReason will always be filled
    connection.getCloseReason.flatMap {
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
      case replaceRequest: ReplaceRequested[F, K, M] =>
        // override old connection with new one, connection count is not changed, and callback is not called
        handleReplace(replaceRequest)
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
                receiveMessage(connection.key, m)
            }
            .guarantee(
              handleConnectionFinish(connection)
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
  type ConnectionWithConflictFlag[F[_], K, M] =
    (HandledConnection[F, K, M], Boolean)

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

  sealed abstract class ReplaceResult
  case object ReplaceFinished               extends ReplaceResult
  case object ConnectionAlreadyDisconnected extends ReplaceResult

  /** Connection which is already handled by connection handler i.e it is registered in registry and handler is subscribed
    * for incoming messages of that connection
    *
    * @param key, key of remote node
    * @param serverAddress, address of the server of remote node. In case of incoming connection it will be different than
    *                       the underlyingConnection remoteAddress, because we will look up the remote address based on the
    *                       `key` in the cluster configuration.
    * @param underlyingConnection, encrypted connection to send and receive messages
    */
  class HandledConnection[F[_]: Concurrent, K, M] private (
      val connectionDirection: HandledConnectionDirection,
      globalCancelToken: Deferred[F, Unit],
      val key: K,
      val serverAddress: InetSocketAddress,
      underlyingConnection: EncryptedConnection[F, K, M],
      closeReason: Deferred[F, HandledConnectionCloseReason]
  ) {
    private val createdAt = Instant.now()

    def age(): FiniteDuration =
      (Instant.now().toEpochMilli() - createdAt.toEpochMilli()).millis

    /** For an incoming connection, this is the remote ephemeral address of the socket
      * for an outgoing connection, it is the remote server address.
      */
    def remoteAddress: InetSocketAddress =
      underlyingConnection.remotePeerInfo._2

    /** For an incoming connection, this is the local server address;
      * for an outgoing connection, it is the local ephemeral address of the socket.
      */
    def localAddress: InetSocketAddress = underlyingConnection.localAddress

    /** The client side address of the TCP socket. */
    def ephemeralAddress: InetSocketAddress =
      connectionDirection match {
        case IncomingConnection => remoteAddress
        case OutgoingConnection => localAddress
      }

    def sendMessage(m: M): F[Unit] = {
      underlyingConnection.sendMessage(m)
    }

    def close: F[Unit] = {
      underlyingConnection.close
    }

    def closeAlreadyClosed: F[Unit] = {
      completeWithReason(RemoteClosed) >> underlyingConnection.close
    }

    def requestReplace(
        newConnection: HandledConnection[F, K, M]
    ): F[ReplaceResult] = {
      ReplaceRequested.requestReplace(newConnection).flatMap { request =>
        closeReason.complete(request).attempt.flatMap {
          case Left(_) =>
            (ConnectionAlreadyDisconnected: ReplaceResult).pure[F]
          case Right(_) =>
            underlyingConnection.close >>
              request.waitForReplaceToFinish.as(ReplaceFinished: ReplaceResult)
        }
      }
    }

    private def completeWithReason(r: HandledConnectionCloseReason): F[Unit] =
      closeReason.complete(r).attempt.void

    def getCloseReason: F[HandledConnectionCloseReason] = closeReason.get

    private def handleIncomingEvent(
        incomingEvent: Option[Either[ConnectionError, M]]
    ): F[Option[M]] = {
      incomingEvent match {
        case Some(Right(m)) => m.some.pure[F]
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
    class ReplaceRequested[F[_]: Sync, K, M](
        val newConnection: HandledConnection[F, K, M],
        replaced: Deferred[F, Unit]
    ) extends HandledConnectionCloseReason {
      def signalReplaceSuccess: F[Unit]   = replaced.complete(()).attempt.void
      def waitForReplaceToFinish: F[Unit] = replaced.get
    }

    object ReplaceRequested {
      def requestReplace[F[_]: Concurrent, K, M](
          newConnection: HandledConnection[F, K, M]
      ): F[ReplaceRequested[F, K, M]] = {
        for {
          signal <- Deferred[F, Unit]
        } yield new ReplaceRequested(newConnection, signal)
      }
    }

    sealed abstract class HandledConnectionDirection
    case object IncomingConnection extends HandledConnectionDirection
    case object OutgoingConnection extends HandledConnectionDirection

    private def buildLifeCycleListener[F[_]: Concurrent]
        : F[Deferred[F, HandledConnectionCloseReason]] = {
      for {
        closeReason <- Deferred[F, HandledConnectionCloseReason]
      } yield closeReason
    }

    private[ConnectionHandler] def outgoing[F[_]: Concurrent, K, M](
        globalCancelToken: Deferred[F, Unit],
        encryptedConnection: EncryptedConnection[F, K, M]
    ): F[HandledConnection[F, K, M]] = {
      buildLifeCycleListener[F].map { closeReason =>
        new HandledConnection[F, K, M](
          OutgoingConnection,
          globalCancelToken,
          encryptedConnection.remotePeerInfo._1,
          encryptedConnection.remotePeerInfo._2,
          encryptedConnection,
          closeReason
        ) {}
      }
    }

    private[ConnectionHandler] def incoming[F[_]: Concurrent, K, M](
        globalCancelToken: Deferred[F, Unit],
        serverAddress: InetSocketAddress,
        encryptedConnection: EncryptedConnection[F, K, M]
    ): F[HandledConnection[F, K, M]] = {
      buildLifeCycleListener[F].map { closeReason =>
        new HandledConnection[F, K, M](
          IncomingConnection,
          globalCancelToken,
          encryptedConnection.remotePeerInfo._1,
          serverAddress,
          encryptedConnection,
          closeReason
        ) {}
      }
    }

  }

  private def buildHandler[F[_]: Concurrent: ContextShift, K, M](
      connectionFinishCallback: FinishedConnection[K] => F[Unit],
      oppositeConnectionOverlap: FiniteDuration
  )(implicit
      tracers: NetworkTracers[F, K, M]
  ): F[ConnectionHandler[F, K, M]] = {
    for {
      cancelToken         <- Deferred[F, Unit]
      acquiredConnections <- ConnectionsRegister.empty[F, K, M]
      messageQueue        <- ConcurrentQueue.unbounded[F, MessageReceived[K, M]]()
      connectionQueue <- ConcurrentQueue
        .unbounded[F, ConnectionWithConflictFlag[F, K, M]]()
    } yield new ConnectionHandler[F, K, M](
      connectionQueue,
      acquiredConnections,
      messageQueue,
      cancelToken,
      connectionFinishCallback,
      oppositeConnectionOverlap
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
      connectionFinishCallback: FinishedConnection[K] => F[Unit],
      oppositeConnectionOverlap: FiniteDuration
  )(implicit
      tracers: NetworkTracers[F, K, M]
  ): Resource[F, ConnectionHandler[F, K, M]] = {
    Resource
      .make(
        buildHandler[F, K, M](
          connectionFinishCallback,
          oppositeConnectionOverlap
        )
      ) { handler =>
        handler.shutdown
      }
      .flatMap { handler =>
        for {
          _ <- handler.handleConnections.background
        } yield handler
      }
  }

}
