package io.iohk.metronome.examples.robot.app

import cats.effect.Resource
import io.iohk.metronome.crypto.ECPublicKey
import io.iohk.metronome.hotstuff.service.messages.DuplexMessage
import io.iohk.metronome.examples.robot.RobotAgreement
import io.iohk.metronome.examples.robot.service.messages.RobotMessage
import io.iohk.metronome.examples.robot.app.config.{RobotConfig, RobotOptions}
import io.iohk.metronome.networking.{RemoteConnectionManager, ConnectionHandler}
import java.net.InetSocketAddress
import monix.eval.Task
import monix.tail.Iterant

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

  def apply(
      config: RobotConfig,
      opts: RobotOptions
  ): Resource[Task, ConnectionManager] =
    Resource.pure[Task, ConnectionManager] {
      val localNode = config.network.nodes(opts.nodeIndex)

      new RemoteConnectionManager[Task, ECPublicKey, NetworkMessage] {
        override val getLocalPeerInfo: (ECPublicKey, InetSocketAddress) =
          (
            localNode.publicKey,
            new InetSocketAddress(localNode.host, localNode.port)
          )

        override def getAcquiredConnections: Task[Set[ECPublicKey]] = Task {
          config.network.nodes.map(_.publicKey).toSet - localNode.publicKey
        }

        override def incomingMessages: Iterant[
          Task,
          ConnectionHandler.MessageReceived[ECPublicKey, NetworkMessage]
        ] = Iterant.never

        override def sendMessage(
            recipient: ECPublicKey,
            message: NetworkMessage
        ): Task[Either[ConnectionHandler.ConnectionAlreadyClosedException[
          ECPublicKey
        ], Unit]] = Task.now(Right(()))

      }
    }
}
