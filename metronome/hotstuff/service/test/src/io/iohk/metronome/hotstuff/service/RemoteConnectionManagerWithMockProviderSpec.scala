package io.iohk.metronome.hotstuff.service

import cats.effect.Resource
import io.iohk.metronome.hotstuff.service.RemoteConnectionManager.{
  ClusterConfig,
  ConnectionAlreadyClosedException,
  RetryConfig
}
import io.iohk.metronome.hotstuff.service.RemoteConnectionManagerTestUtils._
import io.iohk.metronome.hotstuff.service.RemoteConnectionManagerWithMockProviderSpec._
import monix.eval.Task
import monix.execution.Scheduler
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.net.InetSocketAddress
import scala.concurrent.duration._
import MockEncryptedConnectionProvider._

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
            _                   <- Task.sleep(800.milliseconds)
            stats               <- provider.getStatistics
            acquiredConnections <- connectionManager.getAcquiredConnections
          } yield {
            assert(stats.maxInFlightConnections == 1)
            assert(stats.connectionCounts.get(defaultToMake).contains(3))
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
        connectionToMake = connectionToMake
      )
        .use { connectionManager =>
          for {
            _                   <- Task.sleep(800.milliseconds)
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
    buildTestCaseWithNOutgoingPeers(4)
  ) { case (provider, manager) =>
    for {
      stats               <- provider.getStatistics
      acquiredConnections <- manager.getAcquiredConnections
    } yield {
      assert(stats.maxInFlightConnections == 1)
      assert(acquiredConnections.size == 4)
    }
  }

  it should "send messages to online peers" in customTestCaseResourceT(
    buildTestCaseWithNOutgoingPeers(4)
  ) { case (provider, manager) =>
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
    buildTestCaseWithNOutgoingPeers(2)
  ) { case (provider, manager) =>
    for {
      acquiredConnections <- manager.getAcquiredConnections
      disconnectedPeer    <- provider.randomPeerDisconnect()
      _                   <- manager.waitForNConnections(1)
      _ <- manager.getAcquiredConnections.map(keys =>
        assert(!keys.contains(disconnectedPeer))
      )
      _ <- provider.registerOnlinePeer(disconnectedPeer)
      _ <- manager.waitForNConnections(2)
      _ <- manager.getAcquiredConnections.map(keys =>
        assert(keys.contains(disconnectedPeer))
      )
      acquiredConnections1 <- manager.getAcquiredConnections
      stats                <- provider.getStatistics
    } yield {
      assert(stats.maxInFlightConnections == 1)
      assert(acquiredConnections.size == 2)
      assert(acquiredConnections1.size == 2)
    }
  }

  it should "try to reconnect to failed peer after failed send" in customTestCaseResourceT(
    buildTestCaseWithNOutgoingPeers(2)
  ) { case (provider, manager) =>
    for {
      acquiredConnections <- manager.getAcquiredConnections
      disconnectedPeer    <- provider.failRandomPeer()
      _                   <- Task.sleep(100.milliseconds)
      // remote peer failed without any notice, we still have it in our acquired connections
      _ <- manager.getAcquiredConnections.map(keys =>
        assert(keys.contains(disconnectedPeer))
      )
      sendResult <- manager
        .sendMessage(disconnectedPeer, MessageA(1))
        .attempt
        .map(result => result.left.getOrElse(null))
      _ <- Task(
        assert(sendResult == ConnectionAlreadyClosedException(disconnectedPeer))
      )
      _ <- manager.getAcquiredConnections.map(keys =>
        assert(!keys.contains(disconnectedPeer))
      )
      _ <- provider.registerOnlinePeer(disconnectedPeer)
      _ <- manager.waitForNConnections(2)
      _ <- manager.getAcquiredConnections.map(keys =>
        assert(keys.contains(disconnectedPeer))
      )
      stats <- provider.getStatistics
    } yield {
      assert(stats.maxInFlightConnections == 1)
      assert(acquiredConnections.size == 2)
    }
  }

  it should "deny not allowed incoming connections " in customTestCaseResourceT(
    buildTestCaseWithNOutgoingPeers(2)
  ) { case (provider, manager) =>
    for {
      acquiredConnections <- manager.getAcquiredConnections
      incomingPeerConnection <- provider.newIncomingPeer(
        Secp256k1Key.getFakeRandomKey
      )
      _ <- Task.sleep(100.milliseconds)
      _ <- manager.getAcquiredConnections.map(connections =>
        assert(!connections.contains(incomingPeerConnection.remotePeerInfo._1))
      )
      closedIncoming <- incomingPeerConnection.isClosed
      stats          <- provider.getStatistics
    } yield {
      assert(stats.maxInFlightConnections == 1)
      assert(acquiredConnections.size == 2)
      assert(closedIncoming)
    }
  }

  it should "allow configured incoming connections" in customTestCaseResourceT(
    buildTestCaseWithNOutgoingPeers(2)
  ) { case (provider, manager) =>
    for {
      incomingPeerConnection <- provider.newIncomingPeer(defalutAllowed)
      _                      <- Task.sleep(100.milliseconds)
      _ <- manager.getAcquiredConnections.map(connections =>
        assert(connections.contains(incomingPeerConnection.remotePeerInfo._1))
      )
      closedIncoming      <- incomingPeerConnection.isClosed
      acquiredConnections <- manager.getAcquiredConnections
      stats               <- provider.getStatistics
    } yield {
      assert(stats.maxInFlightConnections == 1)
      assert(acquiredConnections.size == 3)
      assert(!closedIncoming)
    }
  }

  it should "not reconnect to incoming connections" in customTestCaseResourceT(
    buildTestCaseWithNOutgoingPeers(2)
  ) { case (provider, manager) =>
    for {
      incomingPeerConnection <- provider.newIncomingPeer(defalutAllowed)
      _                      <- Task.sleep(100.milliseconds)
      _ <- manager.getAcquiredConnections.map(connections =>
        assert(connections.contains(incomingPeerConnection.remotePeerInfo._1))
      )
      acquiredConnections                <- manager.getAcquiredConnections
      _                                  <- provider.specificPeerDisconnect(defalutAllowed)
      _                                  <- manager.waitForNConnections(2)
      _                                  <- provider.registerOnlinePeer(defalutAllowed)
      _                                  <- Task.sleep(1.second)
      acquiredConnectionsAfterDisconnect <- manager.getAcquiredConnections

    } yield {
      assert(acquiredConnections.size == 3)
      assert(acquiredConnectionsAfterDisconnect.size == 2)
    }
  }

  it should "receive messages from all connections" in customTestCaseResourceT(
    buildTestCaseWithNOutgoingPeers(2)
  ) { case (provider, manager) =>
    for {
      incomingPeerConnection <- provider.newIncomingPeer(defalutAllowed)
      _                      <- Task.sleep(100.milliseconds)
      _ <- manager.getAcquiredConnections.map(connections =>
        assert(connections.contains(incomingPeerConnection.remotePeerInfo._1))
      )
      acquiredConnections <- manager.getAcquiredConnections
      connections         <- provider.getAllRegisteredPeers
      _ <- Task.traverse(connections)(conn =>
        conn.pushRemoteEvent(Some(Right(MessageA(1))))
      )
      received <- manager.incomingMessages.take(3).toListL
    } yield {
      assert(acquiredConnections.size == 3)
      assert(received.size == 3)
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
  }

  def buildTestCaseWithNOutgoingPeers(
      n: Int
  )(implicit timeOut: FiniteDuration): Resource[
    Task,
    (
        MockEncryptedConnectionProvider,
        RemoteConnectionManager[Task, Secp256k1Key, TestMessage]
    )
  ] = {
    val keys = (0 until n).map(_ => (Secp256k1Key.getFakeRandomKey)).toSet

    for {
      provider <- Resource.liftF(MockEncryptedConnectionProvider())
      onlineConnections <- Resource.liftF(
        Task
          .traverse(keys)(key => provider.registerOnlinePeer(key))
          .flatMap(_ => provider.getAllRegisteredPeers)
      )
      manager <- buildConnectionsManagerWithMockProvider(
        provider,
        connectionToMake = onlineConnections.map(conn => conn.remotePeerInfo)
      )
      _ <- Resource.liftF(manager.waitForNConnections(n))
    } yield (provider, manager)
  }

  val fakeLocalAddress = new InetSocketAddress("localhost", 127)

  val defalutAllowed = Secp256k1Key.getFakeRandomKey
  val defaultToMake  = Secp256k1Key.getFakeRandomKey

  def buildConnectionsManagerWithMockProvider(
      ec: MockEncryptedConnectionProvider,
      retryConfig: RetryConfig = RetryConfig(50.milliseconds, 2, 2.seconds),
      connectionToMake: Set[(Secp256k1Key, InetSocketAddress)] = Set(
        (defaultToMake, fakeLocalAddress)
      ),
      allowedIncoming: Set[Secp256k1Key] = Set(defalutAllowed)
  ): Resource[
    Task,
    RemoteConnectionManager[Task, Secp256k1Key, TestMessage]
  ] = {
    val clusterConfig = ClusterConfig
      .buildConfig(
        connectionToMake,
        allowedIncoming
      )
      .get

    RemoteConnectionManager(ec, clusterConfig, retryConfig)
  }

}
