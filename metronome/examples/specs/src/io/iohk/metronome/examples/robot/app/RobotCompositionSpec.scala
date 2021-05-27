package io.iohk.metronome.examples.robot.app

import cats.implicits._
import cats.effect.Resource
import io.iohk.metronome.crypto.ECKeyPair
import io.iohk.metronome.hotstuff.consensus.basic.Phase
import io.iohk.metronome.hotstuff.service.tracing.ConsensusEvent
import io.iohk.metronome.examples.robot.RobotAgreement
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
import org.scalatest.matchers.should.Matchers
import org.scalatest.Inspectors

/** Set up an in-memory federation with simulated network stack and elapsed time. */
class RobotIntegrationSpec extends AnyFlatSpec with Matchers with Inspectors {
  import RobotIntegrationSpec._
  import RobotTestConnectionManager.{Dispatcher, Delay, Loss}

  def test(fixture: Fixture): Assertion = {
    implicit val scheduler = fixture.scheduler

    val fut = fixture.resources.use { case (dispatcher, envs) =>
      // Without an extra delay, the `TestScheduler` executes tasks immediately.
      fixture.test(dispatcher, envs).delayExecution(0.second)
    }.runToFuture

    scheduler.tick(fixture.duration)

    // Check that no fiber raised an unhandled error.
    scheduler.state.lastReportedError shouldBe null

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

  behavior of "RobotAgreement"

  it should "compose components that can run and stay in sync" in test {
    // This is a happy scenario, all nodes starting at the same time and
    // running flawlessly, so we should see consensus very quickly.
    new Fixture(
      1.minutes,
      networkDelay = Delay(min = 50.millis, max = 500.millis),
      networkLoss = Loss(0.01)
    ) {
      override def test(
          dispatcher: Dispatcher,
          envs: List[RobotTestComposition.Env]
      ) =
        for {
          _    <- Task.sleep(duration - 5.seconds)
          logs <- envs.traverse(_.logTracer.getLogs)

          quourumCounts <- envs.traverse(
            _.consensusEventTracer
              .count[ConsensusEvent.Quorum[RobotAgreement]]
          )
          blockCounts <- envs.traverse(
            _.consensusEventTracer
              .count[ConsensusEvent.BlockExecuted[RobotAgreement]]
          )

          lastCommittedBlockHashes <- envs
            .traverse { env =>
              env.consensusEventTracer.getEvents.map { events =>
                events.reverse.collectFirst {
                  case ConsensusEvent.Quorum(qc) if qc.phase == Phase.Commit =>
                    qc.blockHash
                }
              }
            }
            .map(_.flatten)

          lastExecutedBlockHashes <- envs.traverse { env =>
            env.storages.storeRunner.runReadOnly {
              env.storages.viewStateStorage.getLastExecutedBlockHash
            }
          }
        } yield {
          // printLogs(logs)
          all(quourumCounts) should be > 0
          all(blockCounts) should be > 0
          // Check that consensus is reasonably close on nodes.
          lastExecutedBlockHashes.distinct.size should be <= 2
          lastCommittedBlockHashes.distinct.size should be <= 2
          // Hopefully someone has executed the last commit as well.
          forAtLeast(
            1,
            lastCommittedBlockHashes
          ) { lastCommittedBlockHash =>
            lastExecutedBlockHashes.contains(
              lastCommittedBlockHash
            ) shouldBe true
          }
        }
    }
  }

  it should "be able to sync when nodes go offline" in test {
    // In this scenario, networking is disabled on all nodes in the
    // beginning, then they are enabled one by one. By the end they
    // should reconnect and sync with each other.
    new Fixture(
      360.seconds,
      networkDelay = Delay(min = 50.millis, max = 250.millis)
    ) {
      val staggerDuration = 30.seconds

      override def test(
          dispatcher: Dispatcher,
          envs: List[RobotTestComposition.Env]
      ) =
        for {
          keys <- dispatcher.connectionPublicKeys.map(_.toList)
          _    <- keys.traverse(dispatcher.disable)
          // NOTE: keys.traverse doesn't seem to work with the TestScheduler and sleep.
          _ <- keys.foldLeft(Task.unit) { case (task, key) =>
            task >> Task.sleep(staggerDuration) >> dispatcher.enable(key)
          }
          // Let them sync now.
          _    <- Task.sleep(duration - 5.seconds - keys.length * staggerDuration)
          logs <- envs.traverse(_.logTracer.getLogs)

          quourumCounts <- envs.traverse(
            _.consensusEventTracer
              .count[ConsensusEvent.Quorum[RobotAgreement]]
          )

          adoptCounts <- envs.traverse(
            _.consensusEventTracer
              .count[ConsensusEvent.AdoptView[RobotAgreement]]
          )

          viewNumbers <- envs.traverse { env =>
            env.storages.storeRunner.runReadOnly {
              env.storages.viewStateStorage.getBundle.map(_.viewNumber)
            }
          }
        } yield {
          //printLogs(logs)
          all(quourumCounts) should be > 0
          atLeast(1, adoptCounts) should be > 0
          viewNumbers.distinct.size should be <= 2
        }
    }
  }
}

object RobotIntegrationSpec {

  import RobotTestConnectionManager.{Delay, Loss, Dispatcher}

  abstract class Fixture(
      // Maximum time to run the simulated test for.
      // There should be some `Task.sleep` in the `test` to let things unfold
      // before making assertions, but altogether less sleep than `duration`.
      val duration: FiniteDuration,
      networkDelay: Delay = Delay.Zero,
      networkLoss: Loss = Loss.Zero,
      nodeCount: Int = 5
  ) extends RobotComposition {

    /** Override to implement the test. */
    def test(
        dispatcher: Dispatcher,
        envs: List[RobotTestComposition.Env]
    ): Task[Assertion]

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
        // Just generate new keys, ignore what's in the default configuration.
        rnd  = new java.security.SecureRandom()
        keys = List.fill(nodeCount)(ECKeyPair.generate(rnd))

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
        dispatcher <- Resource.liftF {
          Dispatcher(networkDelay, networkLoss)
        }
        nodeEnvs <- (0 until config.network.nodes.size).toList.map { i =>
          val opts = RobotOptions(nodeIndex = i)
          val comp = makeComposition(scheduler, dispatcher)
          comp.composeEnv(opts, config)
        }.sequence
      } yield (dispatcher, nodeEnvs)

    def makeComposition(
        scheduler: TestScheduler,
        dispatcher: RobotTestConnectionManager.Dispatcher
    ) =
      new RobotTestComposition(scheduler, dispatcher)
  }
}
