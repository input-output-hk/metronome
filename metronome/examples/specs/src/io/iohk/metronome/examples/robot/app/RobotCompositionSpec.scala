package io.iohk.metronome.examples.robot.app

import cats.implicits._
import cats.effect.{Blocker, Resource}
import io.iohk.metronome.crypto.{ECKeyPair, ECPublicKey}
import io.iohk.metronome.networking.{
  RemoteConnectionManager,
  ConnectionHandler,
  NetworkTracers
}
import io.iohk.metronome.hotstuff.service.tracing.{
  ConsensusTracers,
  SyncTracers
}
import io.iohk.metronome.examples.robot.app.tracing._
import io.iohk.metronome.examples.robot.app.config.{
  RobotConfig,
  RobotConfigParser,
  RobotOptions
}
import io.iohk.metronome.logging.{InMemoryLogTracer, HybridLog}
import java.nio.file.Files
import java.net.InetSocketAddress
import monix.eval.Task
import monix.execution.schedulers.TestScheduler
import monix.tail.Iterant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.compatible.Assertion
import scala.concurrent.duration._

class RobotCompositionSpec extends AnyFlatSpec {
  import RobotCompositionSpec.Fixture

  def test(fixture: Fixture): Assertion = {
    implicit val scheduler = fixture.scheduler

    // Without an extra delay, the `TestScheduler` executes tasks immediately.
    val fut = fixture.resources.use { _ =>
      fixture.test.delayExecution(0.second)
    }.runToFuture

    scheduler.tick(fixture.duration)

    fut.value.getOrElse(sys.error("The test hasn't finished")).get
  }

  behavior of "RobotComposition"

  it should "compose components that can run and stay in sync" in test {
    new Fixture(10.minutes) {
      // Wait with the test result to keep the resources working.
      override val test =
        Task(succeed).delayResult(duration - 1.minute)
    }
  }
}

object RobotCompositionSpec {
  abstract class Fixture(val duration: FiniteDuration)
      extends RobotComposition {

    /** Override to implement the test. */
    def test: Task[Assertion]

    val scheduler = TestScheduler()

    // TODO: Separate builder and trace for each node.
    val logTracer = InMemoryLogTracer.hybrid[Task]

    def config: Resource[Task, RobotConfig] = {
      for {
        defaultConfig <- Resource.liftF {
          Task.fromEither {
            RobotConfigParser.parse.left.map(err =>
              new IllegalArgumentException(err.toString)
            )
          }
        }
        // Use 5 nodes in integration testing.
        rnd  = new java.security.SecureRandom()
        keys = List.fill(5)(ECKeyPair.generate(rnd))

        tmpdir <- Resource.liftF(Task {
          val tmp = Files.createTempDirectory("robot-testdb")
          tmp.toFile.deleteOnExit()
          tmp
        })

        config = defaultConfig.copy(
          network = defaultConfig.network.copy(
            nodes = keys.zipWithIndex.map { case (pair, i) =>
              RobotConfig.Node(
                host = "localhost",
                port = 40000 + i,
                publicKey = pair.pub,
                privateKey = pair.prv
              )
            }
          ),
          db = defaultConfig.db.copy(
            path = tmpdir
          )
        )
      } yield config
    }

    def resources = for {
      config <- config
      nodes <- (0 until config.network.nodes.size).toList.map { i =>
        val opts = RobotOptions(nodeIndex = i)
        compose(opts, config)
      }.sequence
    } yield ()

    private def makeLogTracer[T: HybridLog] =
      InMemoryLogTracer.hybrid[Task, T](logTracer)

    override protected def makeNetworkTracers = {
      import RobotNetworkTracers._
      NetworkTracers(makeLogTracer[RobotNetworkEvent])
    }

    override protected def makeConsensusTracers = {
      import RobotConsensusTracers._
      ConsensusTracers(makeLogTracer[RobotConsensusEvent])
    }

    override protected def makeSyncTracers = {
      import RobotSyncTracers._
      SyncTracers(makeLogTracer[RobotSyncEvent])
    }

    // Use the `TestScheduler` to block on queries, otherwise the test hangs.
    override protected def makeDBBlocker =
      Resource.pure[Task, Blocker](Blocker.liftExecutionContext(scheduler))

    // TODO: Simulate a network.
    // NOTE: We cannot use a real network with the `TestScheduler`.
    override protected def makeConnectionManager(
        config: RobotConfig,
        opts: RobotOptions
    )(implicit
        networkTracers: NTS
    ) = Resource.pure[Task, ConnectionManager] {
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

}
