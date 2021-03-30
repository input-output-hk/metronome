package io.iohk.metronome.networking

import cats.effect.Resource
import io.iohk.metronome.networking.ConnectionHandler.ConnectionAlreadyClosedException
import io.iohk.metronome.networking.EncryptedConnectionProvider.DecodingError
import io.iohk.metronome.networking.MockEncryptedConnectionProvider._
import io.iohk.metronome.networking.RemoteConnectionManager.RetryConfig.RandomJitterConfig
import io.iohk.metronome.networking.RemoteConnectionManager.{
  ClusterConfig,
  RetryConfig
}
import io.iohk.metronome.networking.RemoteConnectionManagerTestUtils._
import io.iohk.metronome.networking.RemoteConnectionManagerWithMockProviderSpec.{
  RemoteConnectionManagerOps,
  buildConnectionsManagerWithMockProvider,
  buildTestCaseWithNPeers,
  defaultToMake,
  fakeLocalAddress,
  longRetryConfig
}
import io.iohk.metronome.tracer.Tracer
import monix.eval.Task
import monix.execution.Scheduler
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.net.InetSocketAddress
import scala.concurrent.duration._

class RemoteConnectionManagerWithMockProviderSpec
    extends AsyncFlatSpecLike
    with Matchers {
  implicit val testScheduler =
    Scheduler.fixedPool("RemoteConnectionManagerUtSpec", 16)
  implicit val timeOut = 5.seconds

  behavior of "RemoteConnectionManagerWithMockProvider"

  it should "continue to make connections to unresponsive peer with exponential backoff" in customTestCaseT {
    MockEncryptedConnectionProvider().flatMap(provider =>
      buildConnectionsManagerWithMockProvider(provider)
        .use { connectionManager =>
          for {
            _                   <- Task.sleep(1.second)
            stats               <- provider.getStatistics
            acquiredConnections <- connectionManager.getAcquiredConnections
          } yield {
            assert(stats.maxInFlightConnections == 1)
            assert(
              stats.connectionCounts
                .get(defaultToMake)
                .exists(count => count == 2 || count == 3)
            )
            assert(acquiredConnections.isEmpty)
          }
        }
    )
  }

  it should "continue to make connections to unresponsive peers one connection at the time" in customTestCaseT {
    val connectionToMake =
      (0 to 3).map(_ => (Secp256k1Key.getFakeRandomKey, fakeLocalAddress)).toSet
    MockEncryptedConnectionProvider().flatMap(provider =>
      buildConnectionsManagerWithMockProvider(
        provider,
        nodesInCluster = connectionToMake
      )
        .use { connectionManager =>
          for {
            _                   <- Task.sleep(1.second)
            stats               <- provider.getStatistics
            acquiredConnections <- connectionManager.getAcquiredConnections
          } yield {
            assert(
              connectionToMake.forall(connection =>
                stats.connectionCounts
                  .get(connection._1)
                  .exists(count => count == 2 || count == 3)
              )
            )
            assert(stats.maxInFlightConnections == 1)
            assert(acquiredConnections.isEmpty)
          }
        }
    )
  }

  it should "connect to online peers" in customTestCaseResourceT(
    buildTestCaseWithNPeers(4)
  ) { case (provider, manager, _) =>
    for {
      stats               <- provider.getStatistics
      acquiredConnections <- manager.getAcquiredConnections
    } yield {
      assert(stats.maxInFlightConnections == 1)
      assert(acquiredConnections.size == 4)
    }
  }

  it should "send messages to online peers" in customTestCaseResourceT(
    buildTestCaseWithNPeers(4)
  ) { case (provider, manager, _) =>
    for {
      acquiredConnections <- manager.getAcquiredConnections
      _ <- manager.getAcquiredConnections.flatMap(keys =>
        Task.traverse(keys)(key => manager.sendMessage(key, MessageA(2)))
      )
      received <- provider.getReceivedMessagesPerPeer.map(_.map(_._2))
      stats    <- provider.getStatistics
    } yield {
      assert(stats.maxInFlightConnections == 1)
      assert(acquiredConnections.size == 4)
      assert(
        received.forall(peerMessages => peerMessages.contains(MessageA(2)))
      )
    }
  }

  it should "try to reconnect disconnected peer" in customTestCaseResourceT(
    buildTestCaseWithNPeers(2)
  ) { case (provider, manager, _) =>
    for {
      disconnectedPeer <- provider.randomPeerDisconnect()
      _                <- manager.waitForNConnections(1)
      notContainDisconnectedPeer <- manager.notContainsConnection(
        disconnectedPeer
      )
      _                      <- provider.registerOnlinePeer(disconnectedPeer.key)
      _                      <- manager.waitForNConnections(2)
      containsAfterReconnect <- manager.containsConnection(disconnectedPeer)
    } yield {
      assert(notContainDisconnectedPeer)
      assert(containsAfterReconnect)
    }
  }

  it should "try to reconnect to failed peer after failed send" in customTestCaseResourceT(
    buildTestCaseWithNPeers(2)
  ) { case (provider, manager, _) =>
    for {
      disconnectedPeer <- provider.failRandomPeer()
      _                <- Task.sleep(100.milliseconds)
      // remote peer failed without any notice, we still have it in our acquired connections
      containsFailedPeer <- manager.containsConnection(disconnectedPeer)
      sendResult <- manager
        .sendMessage(disconnectedPeer.key, MessageA(1))
        .map(result => result.left.getOrElse(null))
      _ <- Task(
        assert(
          sendResult == ConnectionAlreadyClosedException(disconnectedPeer.key)
        )
      )
      notContainsFailedPeerAfterSend <- manager.notContainsConnection(
        disconnectedPeer
      )
      _ <- provider.registerOnlinePeer(disconnectedPeer.key)
      _ <- manager.waitForNConnections(2)
      containsFailedAfterReconnect <- manager.containsConnection(
        disconnectedPeer
      )
    } yield {
      assert(containsFailedPeer)
      assert(notContainsFailedPeerAfterSend)
      assert(containsFailedAfterReconnect)
    }
  }

  it should "fail sending message to unknown peer" in customTestCaseResourceT(
    buildTestCaseWithNPeers(2)
  ) { case (provider, manager, _) =>
    val randomKey = Secp256k1Key.getFakeRandomKey
    for {
      sendResult <- manager.sendMessage(randomKey, MessageA(1))
    } yield {
      assert(sendResult.isLeft)
      assert(
        sendResult.left.getOrElse(null) == ConnectionAlreadyClosedException(
          randomKey
        )
      )
    }
  }

  it should "deny not allowed incoming connections " in customTestCaseResourceT(
    buildTestCaseWithNPeers(2)
  ) { case (provider, manager, _) =>
    for {
      incomingPeerConnection <- provider.newIncomingPeer(
        Secp256k1Key.getFakeRandomKey
      )
      _ <- Task.sleep(100.milliseconds)
      notContainsNotAllowedIncoming <- manager.notContainsConnection(
        incomingPeerConnection
      )
      closedIncoming <- incomingPeerConnection.isClosed
    } yield {
      assert(notContainsNotAllowedIncoming)
      assert(closedIncoming)
    }
  }

  it should "allow configured incoming connections" in customTestCaseResourceT(
    buildTestCaseWithNPeers(2, shouldBeOnline = false, longRetryConfig)
  ) { case (provider, manager, clusterPeers) =>
    for {
      initialAcquired    <- manager.getAcquiredConnections
      incomingConnection <- provider.newIncomingPeer(clusterPeers.head)
      _                  <- manager.waitForNConnections(1)
      containsIncoming   <- manager.containsConnection(incomingConnection)
    } yield {
      assert(initialAcquired.isEmpty)
      assert(containsIncoming)
    }
  }

  it should "not allow duplicated incoming peer" in customTestCaseResourceT(
    buildTestCaseWithNPeers(2, shouldBeOnline = false, longRetryConfig)
  ) { case (provider, manager, clusterPeers) =>
    for {
      initialAcquired          <- manager.getAcquiredConnections
      incomingConnection       <- provider.newIncomingPeer(clusterPeers.head)
      _                        <- manager.waitForNConnections(1)
      containsIncoming         <- manager.containsConnection(incomingConnection)
      duplicatedIncoming       <- provider.newIncomingPeer(clusterPeers.head)
      _                        <- Task.sleep(500.millis) // Let the offered connection be processed.
      duplicatedIncomingClosed <- duplicatedIncoming.isClosed
    } yield {
      assert(initialAcquired.isEmpty)
      assert(containsIncoming)
      assert(duplicatedIncomingClosed)
    }
  }

  it should "disconnect from peer on which connection error happened" in customTestCaseResourceT(
    buildTestCaseWithNPeers(2)
  ) { case (provider, manager, _) =>
    for {
      initialAcquired          <- manager.getAcquiredConnections
      randomAcquiredConnection <- provider.getAllRegisteredPeers.map(_.head)
      _                        <- randomAcquiredConnection.pushRemoteEvent(Some(Left(DecodingError)))
      _                        <- manager.waitForNConnections(1)
      _                        <- Task.sleep(500.millis) // Let the offered connection be processed.
      errorIsClosed            <- randomAcquiredConnection.isClosed
    } yield {
      assert(initialAcquired.size == 2)
      assert(errorIsClosed)
    }
  }

  it should "receive messages from all connections" in customTestCaseResourceT(
    buildTestCaseWithNPeers(2)
  ) { case (provider, manager, _) =>
    for {
      acquiredConnections <- manager.getAcquiredConnections
      connections         <- provider.getAllRegisteredPeers
      _ <- Task.traverse(connections)(conn =>
        conn.pushRemoteEvent(Some(Right(MessageA(1))))
      )
      received <- manager.incomingMessages.take(2).toListL
    } yield {
      assert(acquiredConnections.size == 2)
      assert(received.size == 2)
    }
  }

}

object RemoteConnectionManagerWithMockProviderSpec {
  implicit class RemoteConnectionManagerOps(
      manager: RemoteConnectionManager[Task, Secp256k1Key, TestMessage]
  ) {
    def waitForNConnections(
        n: Int
    )(implicit timeOut: FiniteDuration): Task[Unit] = {
      manager.getAcquiredConnections
        .restartUntil(connections => connections.size == n)
        .timeout(timeOut)
        .void
    }

    def containsConnection(
        connection: MockEncryptedConnection
    ): Task[Boolean] = {
      manager.getAcquiredConnections.map(connections =>
        connections.contains(connection.remotePeerInfo._1)
      )
    }

    def notContainsConnection(
        connection: MockEncryptedConnection
    ): Task[Boolean] = {
      containsConnection(connection).map(contains => !contains)
    }
  }

  val noJitterConfig = RandomJitterConfig.buildJitterConfig(0).get
  val quickRetryConfig =
    RetryConfig(50.milliseconds, 2, 2.seconds, noJitterConfig)
  val longRetryConfig: RetryConfig =
    RetryConfig(5.seconds, 2, 20.seconds, noJitterConfig)

  def buildTestCaseWithNPeers(
      n: Int,
      shouldBeOnline: Boolean = true,
      retryConfig: RetryConfig = quickRetryConfig
  )(implicit timeOut: FiniteDuration): Resource[
    Task,
    (
        MockEncryptedConnectionProvider,
        RemoteConnectionManager[Task, Secp256k1Key, TestMessage],
        Set[Secp256k1Key]
    )
  ] = {
    val keys = (0 until n).map(_ => (Secp256k1Key.getFakeRandomKey)).toSet

    for {
      provider <- Resource.liftF(MockEncryptedConnectionProvider())
      _ <- Resource.liftF {
        if (shouldBeOnline) {
          Task.traverse(keys)(key => provider.registerOnlinePeer(key))
        } else {
          Task.unit
        }
      }
      manager <- buildConnectionsManagerWithMockProvider(
        provider,
        retryConfig = retryConfig,
        nodesInCluster = keys.map(key => (key, fakeLocalAddress))
      )
      _ <- Resource.liftF {
        if (shouldBeOnline) {
          manager.waitForNConnections(n)
        } else {
          Task.unit
        }
      }
    } yield (provider, manager, keys)
  }

  val fakeLocalAddress = new InetSocketAddress("localhost", 127)

  val defalutAllowed = Secp256k1Key.getFakeRandomKey
  val defaultToMake  = Secp256k1Key.getFakeRandomKey

  implicit val tracers: NetworkTracers[Task, Secp256k1Key, TestMessage] =
    NetworkTracers(Tracer.noOpTracer)

  def buildConnectionsManagerWithMockProvider(
      ec: MockEncryptedConnectionProvider,
      retryConfig: RetryConfig = quickRetryConfig,
      nodesInCluster: Set[(Secp256k1Key, InetSocketAddress)] = Set(
        (defaultToMake, fakeLocalAddress)
      )
  ): Resource[
    Task,
    RemoteConnectionManager[Task, Secp256k1Key, TestMessage]
  ] = {
    val clusterConfig = ClusterConfig(nodesInCluster)

    RemoteConnectionManager(ec, clusterConfig, retryConfig)
  }

}
