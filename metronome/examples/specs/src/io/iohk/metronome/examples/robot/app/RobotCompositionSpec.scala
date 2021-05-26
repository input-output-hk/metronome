package io.iohk.metronome.examples.robot.app

import cats.implicits._
import cats.effect.{Blocker, Resource}
import cats.effect.concurrent.Ref
import io.iohk.metronome.crypto.{ECKeyPair, ECPublicKey}
import io.iohk.metronome.networking.{
  RemoteConnectionManager,
  ConnectionHandler,
  NetworkTracers
}
import io.iohk.metronome.hotstuff.service.tracing.{
  ConsensusEvent,
  ConsensusTracers,
  SyncTracers
}
import io.iohk.metronome.examples.robot.app.tracing._
import io.iohk.metronome.examples.robot.app.config.{
  RobotConfig,
  RobotConfigParser,
  RobotOptions
}
import io.iohk.metronome.logging.{InMemoryLogTracer, HybridLog, HybridLogObject}
import io.iohk.metronome.tracer.Tracer
import java.nio.file.Files
import java.net.InetSocketAddress
import monix.eval.Task
import monix.execution.schedulers.TestScheduler
import monix.tail.Iterant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.compatible.Assertion
import scala.concurrent.duration._
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import scala.reflect.ClassTag

class RobotCompositionSpec extends AnyFlatSpec with Matchers {
  import RobotCompositionSpec._

  def test(fixture: Fixture): Assertion = {
    implicit val scheduler = fixture.scheduler

    // Without an extra delay, the `TestScheduler` executes tasks immediately.
    val fut = fixture.resources.use { envs =>
      fixture.test(envs).delayExecution(0.second)
    }.runToFuture

    scheduler.tick(fixture.duration)

    fut.value.getOrElse(sys.error("The test hasn't finished")).get
  }

  def printLogs(logs: List[Seq[HybridLogObject]]): Unit = {
    logs.zipWithIndex
      .flatMap { case (logs, i) =>
        logs.map(log => (i, log))
      }
      .sortBy(_._2.timestamp)
      .foreach { case (i, log) =>
        println(s"node-$i: ${log.show}")
      }
  }

  def eventCount[A: ClassTag](events: Vector[_]): Int =
    events.collect { case e: A =>
      e
    }.size

  behavior of "RobotComposition"

  it should "compose components that can run and stay in sync" in test {
    new Fixture(10.minutes) {
      // Wait with the test result to keep the resources working.
      override def test(envs: List[Env]) =
        for {
          _               <- Task.sleep(duration - 1.minute)
          logs            <- envs.traverse(_.logTracer.getLogs)
          consensusEvents <- envs.traverse(_.consensusEventTracer.getEvents)
          syncEvents      <- envs.traverse(_.syncEventTracer.getEvents)
        } yield {
          // printLogs(logs)
          Inspectors.forAll(consensusEvents) { events =>
            // Networking isn't yet implemented, so it should just warn about timeouts.
            eventCount[ConsensusEvent.Timeout](events) should be > 0
            eventCount[ConsensusEvent.Quorum[_]](events) shouldBe 0
          }
        }
    }
  }
}

object RobotCompositionSpec {
  import RobotConsensusTracers.RobotConsensusEvent
  import RobotSyncTracers.RobotSyncEvent

  class EventTracer[A](eventLogRef: Ref[Task, Vector[A]])
      extends Tracer[Task, A] {

    override def apply(a: => A): Task[Unit] =
      eventLogRef.update(_ :+ a)

    val clear     = eventLogRef.set(Vector.empty)
    val getEvents = eventLogRef.get
  }
  object EventTracer {
    def apply[A] =
      new EventTracer[A](Ref.unsafe[Task, Vector[A]](Vector.empty))
  }

  /** Things we may want to access in tests. */
  case class Env(
      storages: TestComposition#Storages,
      logTracer: InMemoryLogTracer.HybridLogTracer[Task],
      consensusEventTracer: EventTracer[RobotConsensusEvent],
      syncEventTracer: EventTracer[RobotSyncEvent]
  ) {
    val clear =
      logTracer.clear >> consensusEventTracer.clear >> syncEventTracer.clear
  }

  abstract class Fixture(val duration: FiniteDuration)
      extends RobotComposition {

    /** Override to implement the test. */
    def test(envs: List[Env]): Task[Assertion]

    val scheduler = TestScheduler()

    val config: Resource[Task, RobotConfig] =
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

    val resources =
      for {
        config <- config

        nodeEnvs <- (0 until config.network.nodes.size).toList.map { i =>
          val opts = RobotOptions(nodeIndex = i)
          val comp = makeComposition(scheduler)
          comp.compose(opts, config).map { storages =>
            Env(
              storages,
              comp.logTracer,
              comp.consensusEventTracer,
              comp.syncEventTracer
            )
          }
        }.sequence

        _ <- Resource.pure[Task, Unit](()).onFinalize {
          nodeEnvs.traverse(_.clear).void
        }
      } yield nodeEnvs

    def makeComposition(scheduler: TestScheduler) =
      new TestComposition(scheduler)
  }

  // Every node has its own composer, so we can track logs separately.
  class TestComposition(scheduler: TestScheduler) extends RobotComposition {

    /** Class level log collector.
      *
      * If the composer is reused, this should be cleared between tests.
      */
    val logTracer            = InMemoryLogTracer.hybrid[Task]
    val consensusEventTracer = EventTracer[RobotConsensusEvent]
    val syncEventTracer      = EventTracer[RobotSyncEvent]

    private def makeLogTracer[T: HybridLog] =
      InMemoryLogTracer.hybrid[Task, T](logTracer)

    override protected def makeNetworkTracers = {
      import RobotNetworkTracers._
      NetworkTracers(makeLogTracer[RobotNetworkEvent])
    }

    override protected def makeConsensusTracers = {
      import RobotConsensusTracers._
      ConsensusTracers(
        makeLogTracer[RobotConsensusEvent] |+| consensusEventTracer
      )
    }

    override protected def makeSyncTracers = {
      import RobotSyncTracers._
      SyncTracers(makeLogTracer[RobotSyncEvent] |+| syncEventTracer)
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
