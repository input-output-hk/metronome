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
import scala.concurrent.duration._
import scala.util.Random
import scodec.Codec
import scodec.bits.BitVector

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
  )(implicit codec: Codec[NetworkMessage])
      extends ConnectionManager {

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
    ): Task[Either[AlreadyClosed, Unit]] = for {
      data   <- Task(codec.encode(message).require)
      result <- dispatcher.dispatch(from = publicKey, to = recipient, data)
    } yield result

    def receiveMessage(
        from: ECPublicKey,
        data: BitVector
    ) = for {
      // Since we are in a Fiber, decoding failures are reported to the `TestScheduler`.
      message <- Task(codec.decodeValue(data).require)
      _       <- messageQueue.offer(ConnectionHandler.MessageReceived(from, message))
    } yield ()
  }
  object Connection {

    /** Create a connection for the selected node and register with with the dispatcher.
      *
      * Exercise the codec as well to see if we have anything wrong.
      */
    def apply(
        config: RobotConfig,
        opts: RobotOptions,
        dispatcher: Dispatcher
    )(implicit codec: Codec[NetworkMessage]): Resource[Task, Connection] =
      Resource.make[Task, Connection] {
        for {
          messageQueue <- ConcurrentQueue.unbounded[Task, MessageReceived]()
          localNode  = config.network.nodes(opts.nodeIndex)
          connection = new Connection(dispatcher, localNode, messageQueue)
          _ <- dispatcher.add(connection)
        } yield connection
      }(dispatcher.remove)
  }

  /** Deliver messages to other nodes.
    *
    * Introduce delays and losses into the delivery.
    */
  class Dispatcher(
      connectionsRef: Ref[Task, Map[ECPublicKey, Connection]],
      disabledRef: Ref[Task, Set[ECPublicKey]],
      delay: Delay,
      loss: Loss
  ) {
    val connectionPublicKeys =
      connectionsRef.get.map(_.keySet)

    def dispatch(
        from: ECPublicKey,
        to: ECPublicKey,
        data: BitVector
    ) = {
      val alreadyClosed = ConnectionHandler
        .ConnectionAlreadyClosedException(to)
        .asLeft[Unit]
        .pure[Task]

      for {
        connections <- connectionsRef.get
        disabled    <- disabledRef.get
        result <- connections.get(to) match {
          case _ if disabled(to) || disabled(from) =>
            alreadyClosed

          case None =>
            alreadyClosed

          case Some(_) if loss.next =>
            ().asRight[AlreadyClosed].pure[Task]

          case Some(connection) =>
            connection
              .receiveMessage(from, data)
              .delayExecution(delay.next)
              .startAndForget
              .as(().asRight[AlreadyClosed])
        }
      } yield result
    }

    def add(connection: Connection) =
      connectionsRef.update(_ + (connection.publicKey -> connection))

    def remove(connection: Connection) =
      connectionsRef.update(_ - connection.publicKey)

    def disable(publicKey: ECPublicKey) =
      disabledRef.update(_ + publicKey)

    def enable(publicKey: ECPublicKey) =
      disabledRef.update(_ - publicKey)
  }
  object Dispatcher {
    def apply(delay: Delay = Delay.Zero, loss: Loss = Loss.Zero) =
      for {
        connectionsRef <- Ref.of[Task, Map[ECPublicKey, Connection]](Map.empty)
        disabledRef    <- Ref.of[Task, Set[ECPublicKey]](Set.empty)
      } yield new Dispatcher(connectionsRef, disabledRef, delay, loss)
  }

  case class Delay(min: FiniteDuration, max: FiniteDuration) {
    def next: FiniteDuration =
      min + ((max.toMillis - min.toMillis) * Random.nextDouble()).millis
  }
  object Delay {
    val Zero = Delay(Duration.Zero, Duration.Zero)
  }

  case class Loss(prob: Double) {

    /** Return true if the next call should go lost. */
    def next: Boolean = Random.nextDouble() < prob
  }
  object Loss {
    val Zero = Loss(0)
  }
}
