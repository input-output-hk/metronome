package io.iohk.metronome.examples.robot.app

import cats.implicits._
import cats.effect.Resource
import io.iohk.metronome.crypto.ECKeyPair
import io.iohk.metronome.hotstuff.service.tracing.ConsensusEvent
import io.iohk.metronome.examples.robot.app.config.{
  RobotConfig,
  RobotConfigParser,
  RobotOptions
}
import io.iohk.metronome.logging.HybridLogObject
import java.nio.file.Files
import monix.eval.Task
import monix.execution.schedulers.TestScheduler
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.compatible.Assertion
import scala.concurrent.duration._
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import scala.reflect.ClassTag

/** Set up an in-memory federation with simulated network stack and elapsed time. */
class RobotIntegrationSpec extends AnyFlatSpec with Matchers {
  import RobotIntegrationSpec._

  def test(fixture: Fixture): Assertion = {
    implicit val scheduler = fixture.scheduler

    // Without an extra delay, the `TestScheduler` executes tasks immediately.
    val fut = fixture.resources.use { envs =>
      fixture.test(envs).delayExecution(0.second)
    }.runToFuture

    scheduler.tick(fixture.duration)

    fut.value.getOrElse(sys.error("The test hasn't finished")).get
  }

  // Use this to debug tests.
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
      override def test(envs: List[RobotTestComposition.Env]) =
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

object RobotIntegrationSpec {

  abstract class Fixture(val duration: FiniteDuration)
      extends RobotComposition {

    /** Override to implement the test. */
    def test(envs: List[RobotTestComposition.Env]): Task[Assertion]

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
          comp.composeEnv(opts, config)
        }.sequence
      } yield nodeEnvs

    def makeComposition(scheduler: TestScheduler) =
      new RobotTestComposition(scheduler)
  }
}
