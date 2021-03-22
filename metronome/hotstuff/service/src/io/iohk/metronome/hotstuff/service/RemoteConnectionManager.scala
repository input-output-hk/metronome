package io.iohk.metronome.hotstuff.service

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.implicits._
import cats.effect.{Concurrent, ContextShift, Resource, Sync, Timer}
import cats.implicits._
import io.iohk.metronome.hotstuff.service.EncryptedConnectionProvider.ConnectionError
import io.iohk.metronome.hotstuff.service.RemoteConnectionManager.{
  ConnectionAlreadyClosedException,
  ConnectionsRegister,
  MessageReceived
}
import monix.catnap.ConcurrentQueue
import monix.eval.{TaskLift, TaskLike}
import monix.reactive.Observable
import monix.tail.Iterant
import scodec.Codec

import java.net.InetSocketAddress
import scala.concurrent.duration.FiniteDuration

class RemoteConnectionManager[F[_]: Sync, K, M: Codec](
    acquiredConnections: ConnectionsRegister[F, K, M],
    localInfo: (K, InetSocketAddress),
    concurrentQueue: ConcurrentQueue[F, MessageReceived[K, M]]
) {

  def getLocalInfo: (K, InetSocketAddress) = localInfo

  def getAcquiredConnections: F[Set[K]] = {
    acquiredConnections.getAllRegisteredConnections.map(
      _.map(_.remotePeerInfo._1)
    )
  }

  def incomingMessages: Iterant[F, MessageReceived[K, M]] =
    Iterant.repeatEvalF(concurrentQueue.poll)

  def sendMessage(recipient: K, message: M): F[Unit] = {
    acquiredConnections.getConnection(recipient).flatMap {
      case Some(connection) =>
        //Connections could be closed by remote without us noticing, close it on our side and return error to caller
        connection.sendMessage(message).handleErrorWith { e =>
          //Todo logging
          connection
            .close()
            .flatMap(_ =>
              Sync[F].raiseError(ConnectionAlreadyClosedException(recipient))
            )
        }
      case None =>
        Sync[F].raiseError(ConnectionAlreadyClosedException(recipient))
    }
  }
}
//TODO add logging
object RemoteConnectionManager {

  case class ConnectionAlreadyClosedException[K](key: K)
      extends RuntimeException(
        s"Connection with node ${key}, has already closed"
      )

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

  case class ConnectionSuccess[F[_], K, M](
      encryptedConnection: EncryptedConnection[F, K, M]
  )
  case class ConnectionFailure[K](
      connectionRequest: OutGoingConnectionRequest[K],
      err: Throwable
  )

  private def connectTo[
      F[_]: Sync,
      K: Codec,
      M: Codec
  ](
      encryptedConnectionProvider: EncryptedConnectionProvider[F, K, M],
      connectionRequest: OutGoingConnectionRequest[K]
  ): F[Either[ConnectionFailure[K], ConnectionSuccess[F, K, M]]] = {
    encryptedConnectionProvider
      .connectTo(connectionRequest.key, connectionRequest.address)
      .redeemWith(
        e => Sync[F].pure(Left(ConnectionFailure(connectionRequest, e))),
        connection => Sync[F].pure(Right(ConnectionSuccess(connection)))
      )
  }

  case class RetryConfig(
      initialDelay: FiniteDuration,
      backOffFactor: Long,
      maxDelay: FiniteDuration
  )

  object RetryConfig {
    import scala.concurrent.duration._
    def default: RetryConfig = {
      RetryConfig(500.milliseconds, 2, 30.seconds)
    }
  }

  private def retryConnection[F[_]: Timer: Concurrent, K](
      config: RetryConfig,
      connectionFailure: ConnectionFailure[K]
  ): F[OutGoingConnectionRequest[K]] = {
    // TODO add error logging
    val updatedFailureCount =
      connectionFailure.connectionRequest.numberOfFailures + 1
    val exponentialBackoff =
      math.pow(config.backOffFactor.toDouble, updatedFailureCount).toLong
    val newDelay =
      (config.initialDelay * exponentialBackoff).min(config.maxDelay)

    Timer[F]
      .sleep(newDelay)
      .map(_ =>
        connectionFailure.connectionRequest
          .copy(numberOfFailures = updatedFailureCount)
      )
  }

  /** Connections are acquired in linear fashion i.e there can be at most one concurrent call to remote peer.
    * In case of failure each connection will be retried infinite number of times with exponential backoff between
    * each call.
    */
  private def acquireConnections[
      F[_]: Concurrent: TaskLift: TaskLike: Timer,
      K: Codec,
      M: Codec
  ](
      encryptedConnectionProvider: EncryptedConnectionProvider[F, K, M],
      connectionsToAcquire: ConcurrentQueue[F, OutGoingConnectionRequest[K]],
      connectionsRegister: ConnectionsRegister[F, K, M],
      connectionsQueue: ConcurrentQueue[F, EncryptedConnection[F, K, M]],
      retryConfig: RetryConfig
  ): F[Unit] = {

    /** Observable is used here as streaming primitive as it has richer api than Iterant and have mapParallelUnorderedF
      * combinator, which makes it possible to have multiple concurrent retry timers, which are cancelled when whole
      * outer stream is cancelled
      */
    Observable
      .repeatEvalF(connectionsToAcquire.poll)
      .mapEvalF { connectionToAcquire =>
        connectTo(encryptedConnectionProvider, connectionToAcquire)
      }
      .mapParallelUnorderedF(Integer.MAX_VALUE) {
        case Left(failure) =>
          retryConnection(retryConfig, failure).flatMap(updatedRequest =>
            connectionsToAcquire.offer(updatedRequest)
          )
        case Right(connection) =>
          connectionsRegister
            .registerConnection(connection.encryptedConnection)
            .flatMap(_ =>
              connectionsQueue.offer(connection.encryptedConnection)
            )
      }
      .completedF
  }

  /** Reads incoming connections in linear fashion and check if they are on cluster allowed list.
    */
  private def handleServerConnections[F[_]: Concurrent: TaskLift, K, M: Codec](
      pg: EncryptedConnectionProvider[F, K, M],
      connectionsQueue: ConcurrentQueue[F, EncryptedConnection[F, K, M]],
      connectionsRegister: ConnectionsRegister[F, K, M],
      clusterConfig: ClusterConfig[K]
  ): F[Unit] = {
    Iterant
      .repeatEvalF(pg.incomingConnection)
      .takeWhile(_.isDefined)
      .map(_.get)
      .collect { case Right(value) =>
        value
      }
      .mapEval { encryptedConnection =>
        if (
          clusterConfig.isAllowedIncomingConnection(
            encryptedConnection.remotePeerInfo._1
          )
        ) {
          connectionsRegister
            .isConnectionAlreadyRegistered(encryptedConnection)
            .flatMap { alreadyRegistered =>
              // this case can arise when incoming remote peer fails without giving any notice
              // after restart such peer will try to connect to our local node, even though local node still regards it as
              // connected.
              // this not atomic check is safe as long as incoming connections are processed one by one, and outgoing and
              // incoming connections are disjoint.
              // another option of dealing with this case would be to try to replace old faulty connection with new one.
              if (alreadyRegistered) {
                encryptedConnection.close()
              } else {
                connectionsRegister
                  .registerConnection(encryptedConnection)
                  .flatMap(_ => connectionsQueue.offer(encryptedConnection))
              }
            }
        } else {
          encryptedConnection.close()
        }
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

  private def connectionFinishHandler[F[_]: Concurrent, K, M](
      connection: EncryptedConnection[F, K, M],
      connectionsToAcquire: ConcurrentQueue[F, OutGoingConnectionRequest[K]],
      connectionsRegister: ConnectionsRegister[F, K, M],
      clusterConfig: ClusterConfig[K]
  ): F[Unit] = {

    for {
      _ <- connection.close()
      _ <- connectionsRegister.deregisterConnection(connection)
      _ <-
        if (clusterConfig.isOutGoingConnection(connection.remotePeerInfo._1)) {
          // We have lost one of our outgoing connections, try to re-establish it by pushing it to connection to acquire queue
          connectionsToAcquire.offer(
            OutGoingConnectionRequest.initial(
              connection.remotePeerInfo._1,
              connection.remotePeerInfo._2
            )
          )
        } else {
          Concurrent[F].unit

        }
    } yield ()
  }

  /** Connections multiplexer, it receives both incoming and outgoing connections and start reading incoming messages from
    * them concurrently, putting them on received messages queue.
    * In case of error or stream finish it cleans up all resources.
    */
  private def handleConnections[F[_]: Concurrent: TaskLift, K: Codec, M: Codec](
      connectionQueue: ConcurrentQueue[F, EncryptedConnection[F, K, M]],
      connectionsRegister: ConnectionsRegister[F, K, M],
      connectionsToAcquire: ConcurrentQueue[F, OutGoingConnectionRequest[K]],
      messageQueue: ConcurrentQueue[F, MessageReceived[K, M]],
      clusterConfig: ClusterConfig[K]
  ): F[Unit] = {
    Deferred[F, Unit].flatMap { cancelToken =>
      Iterant
        .repeatEvalF(connectionQueue.poll)
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
                  UnexpectedConnectionError(e, connection.remotePeerInfo._1)
                )
            }
            .guarantee(
              connectionFinishHandler(
                connection,
                connectionsToAcquire,
                connectionsRegister,
                clusterConfig
              )
            )
            .completedL
            .start
        }
        .completedL
        .guarantee(cancelToken.complete(()))
    }
  }

  class ConnectionsRegister[F[_]: Concurrent, K, M: Codec](
      register: Ref[F, Map[K, EncryptedConnection[F, K, M]]]
  ) {

    def registerConnection(
        connection: EncryptedConnection[F, K, M]
    ): F[Unit] = {
      register.update(current =>
        current + (connection.remotePeerInfo._1 -> connection)
      )
    }

    def isConnectionAlreadyRegistered(
        connection: EncryptedConnection[F, K, M]
    ): F[Boolean] = {
      register.get.map(connections =>
        connections.contains(connection.remotePeerInfo._1)
      )
    }

    def deregisterConnection(
        connection: EncryptedConnection[F, K, M]
    ): F[Unit] = {
      register.update(current => current - (connection.remotePeerInfo._1))
    }

    def getAllRegisteredConnections: F[Set[EncryptedConnection[F, K, M]]] = {
      register.get.map(m => m.values.toSet)
    }

    def getConnection(
        connectionKey: K
    ): F[Option[EncryptedConnection[F, K, M]]] =
      register.get.map(connections => connections.get(connectionKey))

  }

  object ConnectionsRegister {
    def empty[F[_]: Concurrent, K, M: Codec]
        : F[ConnectionsRegister[F, K, M]] = {
      Ref
        .of(Map.empty[K, EncryptedConnection[F, K, M]])
        .map(ref => new ConnectionsRegister[F, K, M](ref))
    }
  }

  case class OutGoingConnectionRequest[K](
      key: K,
      address: InetSocketAddress,
      numberOfFailures: Int
  )

  object OutGoingConnectionRequest {
    def initial[K](
        key: K,
        address: InetSocketAddress
    ): OutGoingConnectionRequest[K] = {
      OutGoingConnectionRequest(key, address, 0)
    }
  }

  sealed abstract case class ClusterConfig[K] private (
      connectionsToAcquire: Set[(K, InetSocketAddress)],
      allowedIncoming: Set[K]
  ) {
    def isAllowedIncomingConnection(k: K): Boolean = allowedIncoming.contains(k)

    def isOutGoingConnection(k: K): Boolean = {
      connectionsToAcquire.map(_._1).contains(k)
    }
  }
  object ClusterConfig {
    def empty[K]: ClusterConfig[K] =
      new ClusterConfig[K](Set.empty, Set.empty) {}

    def buildConfig[K](
        connectionsToAcquire: Set[(K, InetSocketAddress)],
        allowedIncoming: Set[K]
    ): Option[ClusterConfig[K]] = {
      val outGoingKeys   = connectionsToAcquire.map(_._1)
      val duplicatedKeys = outGoingKeys.intersect(allowedIncoming).nonEmpty
      if (duplicatedKeys) {
        None
      } else {
        Some(new ClusterConfig[K](connectionsToAcquire, allowedIncoming) {})
      }
    }
  }

  /** Connection manager for static topology cluster. It starts 3 concurrent backgrounds processes:
    * 1. Calling process - tries to connect to remote nodes specified in cluster config. In case of failure, retries with
    *    exponential backoff.
    * 2. Server process - reads incoming connections from server socket. Validates that incoming connections is from known
    *    remote peer specified in cluster config.
    * 3. Message reading process - receives connections from both, Calling and Server processes, and for each connections
    *    start concurrent process reading messages from those connections. In case of some error on connections, it closes
    *    connection. In case of discovering that one of outgoing connections failed, it request Calling process to establish
    *    connection once again.
    *
    * @param encryptedConnectionsProvider component which makes it possible to receive and acquire encrypted connections
    * @param clusterConfig static cluster topology configuration
    * @param retryConfig retry configuration for outgoing connections (incoming connections are not retried)
    */
  def apply[F[_]: Concurrent: TaskLift: TaskLike: Timer, K: Codec, M: Codec](
      encryptedConnectionsProvider: EncryptedConnectionProvider[F, K, M],
      clusterConfig: ClusterConfig[K],
      retryConfig: RetryConfig
  )(implicit
      cs: ContextShift[F]
  ): Resource[F, RemoteConnectionManager[F, K, M]] = {
    for {
      acquiredConnections <- Resource.liftF(ConnectionsRegister.empty[F, K, M])
      connectionsToAcquireQueue <- Resource.liftF(
        ConcurrentQueue.unbounded[F, OutGoingConnectionRequest[K]]()
      )
      _ <- Resource.liftF(
        connectionsToAcquireQueue.offerMany(
          clusterConfig.connectionsToAcquire.map { case (key, address) =>
            OutGoingConnectionRequest.initial(key, address)
          }
        )
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
        connectionQueue,
        retryConfig
      ).background
      _ <- handleServerConnections(
        encryptedConnectionsProvider,
        connectionQueue,
        acquiredConnections,
        clusterConfig
      ).background

      _ <- handleConnections(
        connectionQueue,
        acquiredConnections,
        connectionsToAcquireQueue,
        messageQueue,
        clusterConfig
      ).background
    } yield new RemoteConnectionManager[F, K, M](
      acquiredConnections,
      encryptedConnectionsProvider.localInfo,
      messageQueue
    )

  }
}
