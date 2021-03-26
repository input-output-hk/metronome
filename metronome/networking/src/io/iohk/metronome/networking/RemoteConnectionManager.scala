package io.iohk.metronome.networking

import cats.effect.concurrent.Deferred
import cats.effect.implicits._
import cats.effect.{Concurrent, ContextShift, Resource, Sync, Timer}
import cats.implicits._
import io.iohk.metronome.networking.ConnectionHandler.{
  HandledConnection,
  MessageReceived
}
import monix.catnap.ConcurrentQueue
import monix.eval.{TaskLift, TaskLike}
import monix.reactive.Observable
import monix.tail.Iterant
import scodec.Codec

import java.net.InetSocketAddress
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import scala.concurrent.duration.FiniteDuration

class RemoteConnectionManager[F[_]: Sync, K, M: Codec](
    connectionHandler: ConnectionHandler[F, K, M],
    localInfo: (K, InetSocketAddress)
) {

  def getLocalPeerInfo: (K, InetSocketAddress) = localInfo

  def getAcquiredConnections: F[Set[K]] = {
    connectionHandler.getAllActiveConnections
  }

  def incomingMessages: Iterant[F, MessageReceived[K, M]] =
    connectionHandler.incomingMessages

  def sendMessage(
      recipient: K,
      message: M
  ): F[Either[ConnectionHandler.ConnectionAlreadyClosedException[K], Unit]] = {
    connectionHandler.sendMessage(recipient, message)
  }
}
//TODO add logging
object RemoteConnectionManager {
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
      maxDelay: FiniteDuration,
      maxRandomJitter: Option[FiniteDuration]
  )

  object RetryConfig {
    import scala.concurrent.duration._
    def default: RetryConfig = {
      RetryConfig(500.milliseconds, 2, 30.seconds, maxRandomJitter = None)
    }

  }

  private def retryConnection[F[_]: Timer: Concurrent, K](
      config: RetryConfig,
      failedConnectionRequest: OutGoingConnectionRequest[K]
  ): F[OutGoingConnectionRequest[K]] = {
    val updatedFailureCount =
      failedConnectionRequest.numberOfFailures + 1
    val exponentialBackoff =
      math.pow(config.backOffFactor.toDouble, updatedFailureCount).toLong

    val randomJitter =
      config.maxRandomJitter.fold(FiniteDuration(0, TimeUnit.MILLISECONDS)) {
        maxJitter =>
          val randomJitterInMillis =
            ThreadLocalRandom.current().nextLong(maxJitter.toMillis)
          FiniteDuration(randomJitterInMillis, TimeUnit.MILLISECONDS)
      }

    val newDelay =
      ((config.initialDelay * exponentialBackoff)
        .min(config.maxDelay)) + randomJitter

    Timer[F]
      .sleep(newDelay)
      .as(failedConnectionRequest.copy(numberOfFailures = updatedFailureCount))

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
      connectionsHandler: ConnectionHandler[F, K, M],
      retryConfig: RetryConfig
  ): F[Unit] = {

    /** Observable is used here as streaming primitive as it has richer api than Iterant and have mapParallelUnorderedF
      * combinator, which makes it possible to have multiple concurrent retry timers, which are cancelled when whole
      * outer stream is cancelled
      */
    Observable
      .repeatEvalF(connectionsToAcquire.poll)
      .filterEvalF(request => connectionsHandler.isNewConnection(request.key))
      .mapEvalF { connectionToAcquire =>
        connectTo(encryptedConnectionProvider, connectionToAcquire)
      }
      .mapParallelUnorderedF(Integer.MAX_VALUE) {
        case Left(failure) =>
          //TODO add logging of failure
          val failureToLog = failure.err
          retryConnection(retryConfig, failure.connectionRequest).flatMap(
            updatedRequest => connectionsToAcquire.offer(updatedRequest)
          )
        case Right(connection) =>
          val newOutgoingConnections =
            HandledConnection.outgoing(connection.encryptedConnection)
          connectionsHandler.registerOrClose(newOutgoingConnections)

      }
      .completedF
  }

  /** Reads incoming connections in linear fashion and check if they are on cluster allowed list.
    */
  private def handleServerConnections[F[_]: Concurrent: TaskLift, K, M: Codec](
      pg: EncryptedConnectionProvider[F, K, M],
      connectionsHandler: ConnectionHandler[F, K, M],
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
        clusterConfig.getIncomingConnectionServerInfo(
          encryptedConnection.remotePeerInfo._1
        ) match {
          case Some(incomingConnectionServerAddress) =>
            val handledConnection = HandledConnection.incoming(
              incomingConnectionServerAddress,
              encryptedConnection
            )
            connectionsHandler.registerOrClose(handledConnection)

          case None =>
            // unknown connection, just close it
            encryptedConnection.close
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

  class HandledConnectionFinisher[F[_]: Concurrent: Timer, K, M](
      connectionsToAcquire: ConcurrentQueue[F, OutGoingConnectionRequest[K]],
      retryConfig: RetryConfig
  ) {
    def finish(handledConnection: HandledConnection[F, K, M]): F[Unit] = {
      retryConnection(
        retryConfig,
        OutGoingConnectionRequest.initial(
          handledConnection.key,
          handledConnection.serverAddress
        )
      ).flatMap(req => connectionsToAcquire.offer(req))
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

  case class ClusterConfig[K](
      clusterNodes: Set[(K, InetSocketAddress)]
  ) {
    val clusterNodesKeys = clusterNodes.map(_._1)

    val serverAddresses = clusterNodes.toMap

    def isAllowedIncomingConnection(k: K): Boolean =
      clusterNodesKeys.contains(k)

    def getIncomingConnectionServerInfo(k: K): Option[InetSocketAddress] =
      serverAddresses.get(k)

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
  def apply[
      F[_]: Concurrent: TaskLift: TaskLike: Timer,
      K: Codec,
      M: Codec
  ](
      encryptedConnectionsProvider: EncryptedConnectionProvider[F, K, M],
      clusterConfig: ClusterConfig[K],
      retryConfig: RetryConfig
  )(implicit
      cs: ContextShift[F]
  ): Resource[F, RemoteConnectionManager[F, K, M]] = {
    for {
      connectionsToAcquireQueue <- Resource.liftF(
        ConcurrentQueue.unbounded[F, OutGoingConnectionRequest[K]]()
      )
      _ <- Resource.liftF(
        connectionsToAcquireQueue.offerMany(
          clusterConfig.clusterNodes.collect {
            case toConnect
                if toConnect != encryptedConnectionsProvider.localPeerInfo =>
              OutGoingConnectionRequest.initial(toConnect._1, toConnect._2)
          }
        )
      )

      handledConnectionFinisher = new HandledConnectionFinisher[F, K, M](
        connectionsToAcquireQueue,
        retryConfig
      )

      connectionsHandler <- ConnectionHandler.apply(
        handledConnectionFinisher.finish
      )

      _ <- acquireConnections(
        encryptedConnectionsProvider,
        connectionsToAcquireQueue,
        connectionsHandler,
        retryConfig
      ).background
      _ <- handleServerConnections(
        encryptedConnectionsProvider,
        connectionsHandler,
        clusterConfig
      ).background
    } yield new RemoteConnectionManager[F, K, M](
      connectionsHandler,
      encryptedConnectionsProvider.localPeerInfo
    )

  }
}
