package io.iohk.metronome.networking

import cats.effect.implicits._
import cats.effect.{Concurrent, ContextShift, Resource, Sync, Timer}
import cats.implicits._
import io.iohk.metronome.networking.ConnectionHandler.{
  FinishedConnection,
  MessageReceived
}
import io.iohk.metronome.networking.RemoteConnectionManager.RetryConfig.RandomJitterConfig
import monix.catnap.ConcurrentQueue
import monix.eval.{TaskLift, TaskLike}
import monix.reactive.Observable
import monix.tail.Iterant
import scodec.Codec

import java.net.InetSocketAddress
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import scala.concurrent.duration.FiniteDuration

trait RemoteConnectionManager[F[_], K, M] {
  def getLocalPeerInfo: (K, InetSocketAddress)
  def getAcquiredConnections: F[Set[K]]
  def incomingMessages: Iterant[F, MessageReceived[K, M]]

  /** Send message to a peer, if they are connected.
    *
    * If the recipient is the same as the local peer, the message is immediately
    * delivered to self, so it's important that each recipient has a separate
    * identifier, e.g. we don't try to use the same public key locally on two
    * different ports which are supposed to talk to each other.
    */
  def sendMessage(
      recipient: K,
      message: M
  ): F[Either[ConnectionHandler.ConnectionAlreadyClosedException[K], Unit]]
}

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
      randomJitterConfig: RandomJitterConfig,
      oppositeConnectionOverlap: FiniteDuration
  )

  object RetryConfig {
    sealed abstract case class RandomJitterConfig private (
        fractionOfDelay: Double
    )

    object RandomJitterConfig {
      import scala.concurrent.duration._

      /** Build random jitter config
        * @param fractionOfTheDelay, in what range in the computed jitter should lay, it should by in range 0..1
        */
      def buildJitterConfig(
          fractionOfTheDelay: Double
      ): Option[RandomJitterConfig] = {
        if (fractionOfTheDelay >= 0 && fractionOfTheDelay <= 1) {
          Some(new RandomJitterConfig(fractionOfTheDelay) {})
        } else {
          None
        }
      }

      /** computes new duration with additional random jitter added. Works with millisecond precision i.e if provided duration
        * will be less than 1 millisecond then no jitter will be added
        * @param config,jitter config
        * @param delay, duration to randomize it should positive number otherwise no randomization will happen
        */
      def randomizeWithJitter(
          config: RandomJitterConfig,
          delay: FiniteDuration
      ): FiniteDuration = {
        val fractionDuration =
          (delay.max(0.milliseconds) * config.fractionOfDelay).toMillis
        if (fractionDuration == 0) {
          delay
        } else {
          val randomized = ThreadLocalRandom
            .current()
            .nextLong(-fractionDuration, fractionDuration)
          val randomFactor = FiniteDuration(randomized, TimeUnit.MILLISECONDS)
          delay + randomFactor
        }
      }

      /** Default jitter config which will keep random jitter in +/-20% range
        */
      val defaultConfig: RandomJitterConfig = buildJitterConfig(0.2).get
    }

    import scala.concurrent.duration._
    def default: RetryConfig = {
      RetryConfig(
        initialDelay = 500.milliseconds,
        backOffFactor = 2,
        maxDelay = 30.seconds,
        randomJitterConfig = RandomJitterConfig.defaultConfig,
        oppositeConnectionOverlap = 1.second
      )
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

    val newDelay =
      ((config.initialDelay * exponentialBackoff).min(config.maxDelay))

    val newDelayWithJitter = RandomJitterConfig.randomizeWithJitter(
      config.randomJitterConfig,
      newDelay
    )

    Timer[F]
      .sleep(newDelayWithJitter)
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
  )(implicit tracers: NetworkTracers[F, K, M]): F[Unit] = {

    def connectWithErrors(
        connectionToAcquire: OutGoingConnectionRequest[K]
    ): F[Either[ConnectionFailure[K], Unit]] = {
      connectTo(encryptedConnectionProvider, connectionToAcquire).flatMap {
        case Left(err) =>
          Concurrent[F].pure(Left(err))
        case Right(connection) =>
          connectionsHandler
            .registerOutgoing(connection.encryptedConnection)
            .as(Right(()))
      }
    }

    /** Observable is used here as streaming primitive as it has richer api than Iterant and have mapParallelUnorderedF
      * combinator, which makes it possible to have multiple concurrent retry timers, which are cancelled when whole
      * outer stream is cancelled
      */
    Observable
      .repeatEvalF(connectionsToAcquire.poll)
      .filterEvalF(request => connectionsHandler.isNewConnection(request.key))
      .mapEvalF(connectWithErrors)
      .mapParallelUnorderedF(Integer.MAX_VALUE) {
        case Left(failure) =>
          tracers.failed(failure) >>
            retryConnection(retryConfig, failure.connectionRequest).flatMap(
              updatedRequest => connectionsToAcquire.offer(updatedRequest)
            )
        case Right(_) =>
          Concurrent[F].pure(())
      }
      .completedF
  }

  /** Reads incoming connections in linear fashion and check if they are on cluster allowed list.
    */
  private def handleServerConnections[F[_]: Concurrent: TaskLift, K, M: Codec](
      pg: EncryptedConnectionProvider[F, K, M],
      connectionsHandler: ConnectionHandler[F, K, M],
      clusterConfig: ClusterConfig[K]
  )(implicit tracers: NetworkTracers[F, K, M]): F[Unit] = {
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
            connectionsHandler.registerIncoming(
              incomingConnectionServerAddress,
              encryptedConnection
            )

          case None =>
            // unknown connection, just close it
            tracers.unknown(encryptedConnection) >>
              encryptedConnection.close
        }
      }
      .completedL
  }

  class HandledConnectionFinisher[F[_]: Concurrent: Timer, K, M](
      connectionsToAcquire: ConcurrentQueue[F, OutGoingConnectionRequest[K]],
      retryConfig: RetryConfig
  ) {
    def finish(finishedConnection: FinishedConnection[K]): F[Unit] = {
      retryConnection(
        retryConfig,
        OutGoingConnectionRequest.initial(
          finishedConnection.connectionKey,
          finishedConnection.connectionServerAddress
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

  def apply[F[_]: Sync, K, M: Codec](
      connectionHandler: ConnectionHandler[F, K, M],
      localInfo: (K, InetSocketAddress)
  ): RemoteConnectionManager[F, K, M] = new RemoteConnectionManager[F, K, M] {

    override def getLocalPeerInfo: (K, InetSocketAddress) = localInfo

    override def getAcquiredConnections: F[Set[K]] =
      connectionHandler.getAllActiveConnections

    override def incomingMessages: Iterant[F, MessageReceived[K, M]] =
      connectionHandler.incomingMessages

    override def sendMessage(
        recipient: K,
        message: M
    ): F[
      Either[ConnectionHandler.ConnectionAlreadyClosedException[K], Unit]
    ] = {
      if (recipient == localInfo._1) {
        connectionHandler.receiveMessage(recipient, message).map(_.asRight)
      } else {
        connectionHandler.sendMessage(recipient, message)
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
  def apply[
      F[_]: Concurrent: TaskLift: TaskLike: Timer: ContextShift,
      K: Codec,
      M: Codec
  ](
      encryptedConnectionsProvider: EncryptedConnectionProvider[F, K, M],
      clusterConfig: ClusterConfig[K],
      retryConfig: RetryConfig
  )(implicit
      tracers: NetworkTracers[F, K, M]
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

      connectionsHandler <- ConnectionHandler.apply[F, K, M](
        // when each connection will finished it the callback will be called,
        // and connection will be put to connections to acquire queue
        handledConnectionFinisher.finish,
        // A duration where we consider the possibilty that both nodes opened
        // connections against each other at the same time, and they should try
        // to determinstically pick the same one to close. After this time,
        // we interpret duplicate connections as repairing a failure the other
        // side has detected, but we haven't yet.
        oppositeConnectionOverlap = retryConfig.oppositeConnectionOverlap
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

    } yield RemoteConnectionManager[F, K, M](
      connectionsHandler,
      encryptedConnectionsProvider.localPeerInfo
    )

  }
}
