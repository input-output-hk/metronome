package io.iohk.metronome.hotstuff.service

import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers
import RemoteConnectionManagerTestUtils._
import cats.effect.Resource
import cats.effect.concurrent.{Deferred, Ref, TryableDeferred}
import cats.implicits.toFlatMapOps
import io.iohk.metronome.hotstuff.service.RemoteConnectionManager.{
  ClusterConfig,
  ConnectionAlreadyClosedException,
  RetryConfig
}
import io.iohk.metronome.hotstuff.service.RemoteConnectionManagerUtSpec._
import monix.catnap.ConcurrentQueue
import monix.eval.Task
import monix.execution.Scheduler

import java.net.InetSocketAddress
import scala.concurrent.duration._

class RemoteConnectionManagerUtSpec extends AsyncFlatSpecLike with Matchers {
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

object RemoteConnectionManagerUtSpec {
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
  type IncomingServerEvent = Option[Either[
    EncryptedConnectionProvider.HandshakeFailed,
    EncryptedConnection[Task, Secp256k1Key, TestMessage]
  ]]

  type IncomingConnectionEvent =
    Option[Either[EncryptedConnectionProvider.ChannelError, TestMessage]]

  class MockEncryptedConnection(
      incomingEvents: ConcurrentQueue[Task, IncomingConnectionEvent],
      closeToken: TryableDeferred[Task, Unit],
      sentMessages: Ref[Task, List[TestMessage]],
      val remotePeerInfo: (Secp256k1Key, InetSocketAddress) =
        (Secp256k1Key.getFakeRandomKey, fakeLocalAddress)
  ) extends EncryptedConnection[Task, Secp256k1Key, TestMessage] {

    def pushRemoteEvent(
        ev: Option[
          Either[EncryptedConnectionProvider.ChannelError, TestMessage]
        ]
    ): Task[Unit] = {
      incomingEvents.offer(ev)
    }

    def getReceivedMessages: Task[List[TestMessage]] = sentMessages.get

    // it is possible that in some cases remote peer will be closed without generating final None event in incoming events
    // queue
    def closeRemoteWithoutInfo: Task[Unit] = closeToken.complete(())

    def isClosed: Task[Boolean] =
      closeToken.tryGet.map(closed => closed.isDefined)

    override def close(): Task[Unit] = {
      val closeIfNotClosed = closeToken.tryGet.flatMap {
        case Some(_) => Task.now(())
        case None    => closeToken.complete(())
      }

      Task.parZip2(incomingEvents.offer(None), closeIfNotClosed).void
    }

    override def incomingMessage: Task[IncomingConnectionEvent] =
      incomingEvents.poll

    override def sendMessage(m: TestMessage): Task[Unit] =
      Task
        .race(closeToken.get, sentMessages.update(current => m :: current))
        .flatMap {
          case Left(value) =>
            Task.raiseError(new RuntimeException("Channel already closed"))
          case Right(value) => Task.now(())
        }
  }

  object MockEncryptedConnection {
    def apply(
        remotePeerInfo: (Secp256k1Key, InetSocketAddress) =
          (Secp256k1Key.getFakeRandomKey, fakeLocalAddress)
    ): Task[MockEncryptedConnection] = {
      for {
        incomingEvents <- ConcurrentQueue
          .unbounded[Task, IncomingConnectionEvent]()
        closeToken   <- Deferred.tryable[Task, Unit]
        sentMessages <- Ref.of[Task, List[TestMessage]](List.empty[TestMessage])
      } yield new MockEncryptedConnection(
        incomingEvents,
        closeToken,
        sentMessages,
        remotePeerInfo
      )
    }
  }

  case class ConnectionStatistics(
      inFlightConnections: Long,
      maxInFlightConnections: Long,
      connectionCounts: Map[Secp256k1Key, Long]
  )

  class ConnectionStatisticsHolder(val stats: Ref[Task, ConnectionStatistics]) {
    def incrementInFlight(connectionTo: Secp256k1Key): Task[Unit] = {
      stats.update { current =>
        val newInFlight = current.inFlightConnections + 1
        val newMax =
          if (newInFlight > current.maxInFlightConnections) newInFlight
          else current.maxInFlightConnections

        val newPerConnectionStats =
          current.connectionCounts.get(connectionTo) match {
            case Some(value) =>
              current.connectionCounts.updated(connectionTo, value + 1L)
            case None => current.connectionCounts.updated(connectionTo, 0L)
          }

        ConnectionStatistics(newInFlight, newMax, newPerConnectionStats)
      }
    }

    def decrementInFlight: Task[Unit] = {
      stats.update(current =>
        current.copy(inFlightConnections = current.inFlightConnections - 1)
      )
    }
  }

  class MockEncryptedConnectionProvider(
      val incomingConnections: ConcurrentQueue[Task, IncomingServerEvent],
      val onlineConnections: Ref[
        Task,
        Map[Secp256k1Key, MockEncryptedConnection]
      ],
      val connectionStatistics: ConnectionStatisticsHolder,
      val localInfo: (Secp256k1Key, InetSocketAddress) =
        (Secp256k1Key.getFakeRandomKey, fakeLocalAddress)
  ) extends EncryptedConnectionProvider[Task, Secp256k1Key, TestMessage] {

    private def disconnect(
        withFailure: Boolean,
        chosenPeer: Option[Secp256k1Key] = None
    ): Task[Secp256k1Key] = {
      onlineConnections
        .modify { current =>
          chosenPeer.fold {
            val peer = current.head
            (current - peer._1, peer._2)
          } { keyToFail =>
            val peer = current(keyToFail)
            (current - keyToFail, peer)
          }
        }
        .flatTap { connection =>
          if (withFailure) {
            connection.closeRemoteWithoutInfo
          } else {
            connection.close()
          }
        }
        .map(connection => connection.remotePeerInfo._1)
    }

    def randomPeerDisconnect(): Task[Secp256k1Key] = {
      disconnect(withFailure = false)
    }

    def specificPeerDisconnect(key: Secp256k1Key): Task[Secp256k1Key] = {
      disconnect(withFailure = false, Some(key))
    }

    def failRandomPeer(): Task[Secp256k1Key] = {
      disconnect(withFailure = true)
    }

    def registerOnlinePeer(key: Secp256k1Key): Task[MockEncryptedConnection] = {
      for {
        connection <- MockEncryptedConnection((key, fakeLocalAddress))
        _ <- onlineConnections.update { connections =>
          connections.updated(
            key,
            connection
          )
        }
      } yield connection
    }

    def getAllRegisteredPeers: Task[Set[MockEncryptedConnection]] = {
      onlineConnections.get.map(connections => connections.values.toSet)
    }

    def newIncomingPeer(key: Secp256k1Key): Task[MockEncryptedConnection] = {
      registerOnlinePeer(key).flatMap { connection =>
        incomingConnections.offer(Some(Right(connection))).map(_ => connection)
      }
    }

    def getReceivedMessagesPerPeer
        : Task[Set[(Secp256k1Key, List[TestMessage])]] = {
      onlineConnections.get.flatMap { connections =>
        Task.traverse(connections.toSet) { case (key, connection) =>
          connection.getReceivedMessages.map(received => (key, received))
        }
      }
    }

    def getStatistics: Task[ConnectionStatistics] =
      connectionStatistics.stats.get

    private def connect(k: Secp256k1Key) = {
      onlineConnections.get.flatMap { state =>
        state.get(k) match {
          case Some(value) => Task.now(value)
          case None =>
            Task.raiseError(new RuntimeException("Failed connections"))
        }
      }
    }

    override def connectTo(
        k: Secp256k1Key,
        address: InetSocketAddress
    ): Task[MockEncryptedConnection] = {
      (for {
        _          <- connectionStatistics.incrementInFlight(k)
        connection <- connect(k)
      } yield connection).doOnFinish(_ =>
        connectionStatistics.decrementInFlight
      )
    }

    override def incomingConnection: Task[IncomingServerEvent] =
      incomingConnections.poll
  }

  object MockEncryptedConnectionProvider {
    def apply(): Task[MockEncryptedConnectionProvider] = {
      for {
        queue <- ConcurrentQueue.unbounded[Task, IncomingServerEvent]()
        connections <- Ref.of[Task, Map[Secp256k1Key, MockEncryptedConnection]](
          Map.empty
        )
        connectionsStatistics <- Ref.of[Task, ConnectionStatistics](
          ConnectionStatistics(0, 0, Map.empty)
        )
      } yield new MockEncryptedConnectionProvider(
        queue,
        connections,
        new ConnectionStatisticsHolder(connectionsStatistics)
      )
    }
  }

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
