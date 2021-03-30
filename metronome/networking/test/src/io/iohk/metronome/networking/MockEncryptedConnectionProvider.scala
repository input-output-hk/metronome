package io.iohk.metronome.networking

import cats.effect.concurrent.{Deferred, Ref, TryableDeferred}
import cats.implicits.toFlatMapOps
import io.iohk.metronome.networking.EncryptedConnectionProvider.ConnectionAlreadyClosed
import io.iohk.metronome.networking.MockEncryptedConnectionProvider._
import io.iohk.metronome.networking.RemoteConnectionManagerTestUtils.{
  Secp256k1Key,
  TestMessage
}
import io.iohk.metronome.networking.RemoteConnectionManagerWithMockProviderSpec.fakeLocalAddress
import monix.catnap.ConcurrentQueue
import monix.eval.Task

import java.net.InetSocketAddress

class MockEncryptedConnectionProvider(
    private val incomingConnections: ConcurrentQueue[Task, IncomingServerEvent],
    private val onlineConnections: Ref[
      Task,
      Map[Secp256k1Key, MockEncryptedConnection]
    ],
    private val connectionStatistics: ConnectionStatisticsHolder,
    val localPeerInfo: (Secp256k1Key, InetSocketAddress) =
      (Secp256k1Key.getFakeRandomKey, fakeLocalAddress)
) extends EncryptedConnectionProvider[Task, Secp256k1Key, TestMessage] {

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
    } yield connection).doOnFinish(_ => connectionStatistics.decrementInFlight)
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

  implicit class MockEncryptedConnectionProviderTestMethodsOps(
      provider: MockEncryptedConnectionProvider
  ) {

    private def disconnect(
        withFailure: Boolean,
        chosenPeer: Option[Secp256k1Key] = None
    ): Task[MockEncryptedConnection] = {
      provider.onlineConnections
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
            connection.close
          }
        }
    }

    def randomPeerDisconnect(): Task[MockEncryptedConnection] = {
      disconnect(withFailure = false)
    }

    def specificPeerDisconnect(
        key: Secp256k1Key
    ): Task[MockEncryptedConnection] = {
      disconnect(withFailure = false, Some(key))
    }

    def failRandomPeer(): Task[MockEncryptedConnection] = {
      disconnect(withFailure = true)
    }

    def registerOnlinePeer(key: Secp256k1Key): Task[MockEncryptedConnection] = {
      for {
        connection <- MockEncryptedConnection((key, fakeLocalAddress))
        _ <- provider.onlineConnections.update { connections =>
          connections.updated(
            key,
            connection
          )
        }
      } yield connection
    }

    def getAllRegisteredPeers: Task[Set[MockEncryptedConnection]] = {
      provider.onlineConnections.get.map(connections =>
        connections.values.toSet
      )
    }

    def newIncomingPeer(key: Secp256k1Key): Task[MockEncryptedConnection] = {
      registerOnlinePeer(key).flatMap { connection =>
        provider.incomingConnections
          .offer(Some(Right(connection)))
          .map(_ => connection)
      }
    }

    def getReceivedMessagesPerPeer
        : Task[Set[(Secp256k1Key, List[TestMessage])]] = {
      provider.onlineConnections.get.flatMap { connections =>
        Task.traverse(connections.toSet) { case (key, connection) =>
          connection.getReceivedMessages.map(received => (key, received))
        }
      }
    }

    def getStatistics: Task[ConnectionStatistics] =
      provider.connectionStatistics.stats.get

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

  type IncomingServerEvent = Option[Either[
    EncryptedConnectionProvider.HandshakeFailed,
    EncryptedConnection[Task, Secp256k1Key, TestMessage]
  ]]

  type IncomingConnectionEvent =
    Option[Either[EncryptedConnectionProvider.ConnectionError, TestMessage]]

  class MockEncryptedConnection(
      private val incomingEvents: ConcurrentQueue[
        Task,
        IncomingConnectionEvent
      ],
      private val closeToken: TryableDeferred[Task, Unit],
      private val sentMessages: Ref[Task, List[TestMessage]],
      val remotePeerInfo: (Secp256k1Key, InetSocketAddress) =
        (Secp256k1Key.getFakeRandomKey, fakeLocalAddress)
  ) extends EncryptedConnection[Task, Secp256k1Key, TestMessage] {

    override def close: Task[Unit] = {
      Task
        .parZip2(incomingEvents.offer(None), closeToken.complete(()).attempt)
        .void
    }

    override def incomingMessage: Task[IncomingConnectionEvent] =
      incomingEvents.poll

    override def sendMessage(m: TestMessage): Task[Unit] =
      Task
        .race(closeToken.get, sentMessages.update(current => m :: current))
        .flatMap {
          case Left(_) =>
            Task.raiseError(ConnectionAlreadyClosed(remotePeerInfo._2))
          case Right(_) => Task.now(())
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

    implicit class MockEncryptedConnectionTestMethodsOps(
        connection: MockEncryptedConnection
    ) {
      lazy val key = connection.remotePeerInfo._1

      lazy val address = connection.remotePeerInfo._2

      def pushRemoteEvent(
          ev: Option[
            Either[EncryptedConnectionProvider.ConnectionError, TestMessage]
          ]
      ): Task[Unit] = {
        connection.incomingEvents.offer(ev)
      }

      def getReceivedMessages: Task[List[TestMessage]] =
        connection.sentMessages.get

      // it is possible that in some cases remote peer will be closed without generating final None event in incoming events
      // queue
      def closeRemoteWithoutInfo: Task[Unit] =
        connection.closeToken.complete(())

      def isClosed: Task[Boolean] =
        connection.closeToken.tryGet.map(closed => closed.isDefined)
    }
  }

}
