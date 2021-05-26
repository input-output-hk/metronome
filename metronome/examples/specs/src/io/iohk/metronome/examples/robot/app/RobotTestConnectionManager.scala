package io.iohk.metronome.examples.robot.app

import cats.implicits._
import cats.effect.Resource
import cats.effect.concurrent.Ref
import io.iohk.metronome.crypto.ECPublicKey
import io.iohk.metronome.hotstuff.service.messages.DuplexMessage
import io.iohk.metronome.examples.robot.RobotAgreement
import io.iohk.metronome.examples.robot.service.messages.RobotMessage
import io.iohk.metronome.examples.robot.app.config.{RobotConfig, RobotOptions}
import io.iohk.metronome.networking.{RemoteConnectionManager, ConnectionHandler}
import java.net.InetSocketAddress
import monix.eval.Task
import monix.tail.Iterant
import monix.catnap.ConcurrentQueue

/** Mocked networking stack for integration tests.
  *
  * We can't use the real network with a `TestScheduler` as it would not
  * wait for async events to finish. With a mocked version we can use
  * `Task.sleep` to simulate elapsed time, as well as introduce partitions,
  * message loss, duplicate messages, corrupted messages etc.
  */
object RobotTestConnectionManager {

  type NetworkMessage =
    DuplexMessage[RobotAgreement, RobotMessage]

  type ConnectionManager =
    RemoteConnectionManager[Task, ECPublicKey, NetworkMessage]

  type MessageReceived =
    ConnectionHandler.MessageReceived[ECPublicKey, NetworkMessage]

  type AlreadyClosed =
    ConnectionHandler.ConnectionAlreadyClosedException[ECPublicKey]

  /** Connection manager for test nodes under test.
    *
    * They deliver their messages to the Dispatcher, which simply
    * hands it over to the recipient.
    */
  class Connection(
      dispatcher: Dispatcher,
      localNode: RobotConfig.Node,
      messageQueue: ConcurrentQueue[Task, MessageReceived]
  ) extends ConnectionManager {
    val publicKey = localNode.publicKey

    override val getLocalPeerInfo: (ECPublicKey, InetSocketAddress) =
      (
        localNode.publicKey,
        new InetSocketAddress(localNode.host, localNode.port)
      )

    override def getAcquiredConnections: Task[Set[ECPublicKey]] =
      dispatcher.connectionPublicKeys.map(_ - publicKey)

    override def incomingMessages: Iterant[Task, MessageReceived] =
      Iterant.repeatEvalF(messageQueue.poll)

    override def sendMessage(
        recipient: ECPublicKey,
        message: NetworkMessage
    ): Task[Either[AlreadyClosed, Unit]] =
      dispatcher.dispatch(from = localNode.publicKey, to = recipient, message)

    def receiveMessage(
        from: ECPublicKey,
        message: NetworkMessage
    ) = messageQueue.offer(ConnectionHandler.MessageReceived(from, message))
  }

  /** Deliver messages to other nodes. */
  class Dispatcher(
      connectionsRef: Ref[Task, Map[ECPublicKey, Connection]]
  ) {
    val connectionPublicKeys =
      connectionsRef.get.map(_.values.map(_.getLocalPeerInfo._1).toSet)

    def dispatch(from: ECPublicKey, to: ECPublicKey, message: NetworkMessage) =
      connectionsRef.get.flatMap { connections =>
        connections.get(to) match {
          case None =>
            ConnectionHandler
              .ConnectionAlreadyClosedException(to)
              .asLeft[Unit]
              .pure[Task]

          case Some(connection) =>
            connection
              .receiveMessage(from, message)
              .as(().asRight[AlreadyClosed])
        }
      }

    def add(connection: Connection) =
      connectionsRef.update(_ + (connection.getLocalPeerInfo._1 -> connection))

    def remove(connection: Connection) =
      connectionsRef.update(_ - connection.publicKey)
  }
  object Dispatcher {
    val empty =
      Ref
        .of[Task, Map[ECPublicKey, Connection]](Map.empty)
        .map(new Dispatcher(_))
  }

  /** Create a connection for the selected node and register with with the dispatcher. */
  def apply(
      config: RobotConfig,
      opts: RobotOptions,
      dispatcher: Dispatcher
  ): Resource[Task, Connection] =
    Resource.make[Task, Connection] {
      for {
        messageQueue <- ConcurrentQueue.unbounded[Task, MessageReceived]()
        localNode  = config.network.nodes(opts.nodeIndex)
        connection = new Connection(dispatcher, localNode, messageQueue)
        _ <- dispatcher.add(connection)
      } yield connection
    }(dispatcher.remove)
}