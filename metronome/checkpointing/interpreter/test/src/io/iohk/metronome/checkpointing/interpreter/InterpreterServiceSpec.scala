package io.iohk.metronome.checkpointing.interpreter

import cats.effect.Resource
import io.iohk.metronome.crypto.{ECKeyPair, ECPublicKey}
import io.iohk.metronome.networking.{
  LocalConnectionManager,
  ScalanetConnectionProvider,
  RemoteConnectionManager,
  NetworkTracers,
  NetworkEvent
}
import io.iohk.metronome.checkpointing.interpreter.messages.InterpreterMessage
import io.iohk.metronome.checkpointing.interpreter.codecs.DefaultInterpreterCodecs
import io.iohk.scalanet.peergroup.dynamictls.DynamicTLSPeerGroup
import io.iohk.metronome.tracer.Tracer
import java.net.{InetSocketAddress, ServerSocket}
import java.security.SecureRandom
import monix.eval.Task
import monix.execution.Scheduler
import monix.tail.Iterant
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.compatible.Assertion
import scala.concurrent.Future
import scala.concurrent.duration._
import io.iohk.metronome.checkpointing.models.{Block, Ledger, Transaction}
import io.iohk.metronome.checkpointing.models.{Block, Ledger}
import io.iohk.metronome.checkpointing.models.CheckpointCertificate
import io.iohk.metronome.checkpointing.interpreter.tracing.InterpreterEvent
import org.scalatest.matchers.should.Matchers
import java.time.Instant

class InterpreterServiceSpec extends AsyncFlatSpec with Matchers {
  import InterpreterServiceSpec.Fixture

  def test(fixture: Fixture): Future[Assertion] = {
    import Scheduler.Implicits.global
    fixture.resources.use(fixture.test).timeout(20.seconds).runToFuture
  }

  behavior of "InterpreterService"

  it should "send messages from the Interpreter to the Service" in test {
    new Fixture {
      override def test(input: Input): Task[Assertion] =
        for {
          _ <- input.serviceRpc.newCheckpointCandidate
          m <- input.nextServiceMessage
        } yield {
          m shouldBe an[InterpreterMessage.NewCheckpointCandidateRequest]
        }
    }
  }
}

object InterpreterServiceSpec {

  class MockInterpreterRPC extends InterpreterRPC[Task] {
    override def createBlockBody(
        ledger: Ledger,
        mempool: Seq[Transaction.ProposerBlock]
    ): Task[Option[Block.Body]] = Task.pure(None)

    override def validateBlockBody(
        blockBody: Block.Body,
        ledger: Ledger
    ): Task[Option[Boolean]] = Task.pure(None)

    override def newCheckpointCertificate(
        checkpointCertificate: CheckpointCertificate
    ): Task[Unit] = Task.unit

  }

  abstract class Fixture {
    case class Input(
        serviceConsumer: Iterant.Consumer[Task, InterpreterMessage],
        serviceRpc: ServiceRPC[Task]
    ) {
      def nextServiceMessage: Task[InterpreterMessage] =
        serviceConsumer.pull.flatMap {
          case Left(Some(ex)) =>
            Task.raiseError(ex)
          case Left(None) =>
            Task.raiseError(
              new NoSuchElementException("The message stream ended.")
            )
          case Right(msg) =>
            Task.pure(msg)
        }
    }

    def test(input: Input): Task[Assertion]

    lazy val interpreterRpc: InterpreterRPC[Task] = new MockInterpreterRPC()

    implicit lazy val interpreterTracer: Tracer[Task, InterpreterEvent] =
      Tracer.noOpTracer[Task, InterpreterEvent]

    val resources: Resource[Task, Input] =
      for {
        (serviceAddress, interpreterAddress) <- Resource.liftF(
          randomAddressPair
        )
        rnd = new SecureRandom()
        List(serviceKeyPair, interpreterKeyPair) = List.fill(2) {
          ECKeyPair.generate(rnd)
        }

        serviceConnection <- makeLocalConnectionManager(
          localKeyPair = serviceKeyPair,
          localAddress = serviceAddress,
          targetKeyPair = interpreterKeyPair,
          targetAddress = interpreterAddress
        )

        interpreterConnection <- makeLocalConnectionManager(
          localKeyPair = interpreterKeyPair,
          localAddress = interpreterAddress,
          targetKeyPair = serviceKeyPair,
          targetAddress = serviceAddress
        )

        _ <- Resource.liftF {
          for {
            // Wait some time for initial registration conflicts to be resolved.
            _ <- Task.sleep(1.second)
            _ <- serviceConnection.isConnected
              .restartUntil(identity)
              .timeout(5.seconds)
            _ <- interpreterConnection.isConnected
              .restartUntil(identity)
              .timeout(5.seconds)
          } yield ()
        }

        serviceRpc <- InterpreterService[Task](
          interpreterConnection,
          interpreterRpc,
          timeout = 1.second
        )

        serviceConsumer <- serviceConnection.incomingMessages.consume

      } yield Input(serviceConsumer, serviceRpc)
  }

  val randomAddressPair: Task[(InetSocketAddress, InetSocketAddress)] = Task {
    val s1 = new ServerSocket(0)
    try {
      val s2 = new ServerSocket(0)
      try {
        val a1 = new InetSocketAddress("localhost", s1.getLocalPort)
        val a2 = new InetSocketAddress("localhost", s2.getLocalPort)
        a1 -> a2
      } finally {
        s2.close()
      }
    } finally {
      s1.close()
    }
  }

  def makeLocalConnectionManager(
      localKeyPair: ECKeyPair,
      localAddress: InetSocketAddress,
      targetKeyPair: ECKeyPair,
      targetAddress: InetSocketAddress
  ): Resource[Task, LocalConnectionManager[
    Task,
    ECPublicKey,
    InterpreterMessage
  ]] = {
    import DefaultInterpreterCodecs.interpreterMessageCodec

    val retryConfig = RemoteConnectionManager.RetryConfig.default.copy(
      initialDelay = 500.millis,
      maxDelay = 5.seconds
    )

    implicit val networkTracers = NetworkTracers(
      //Tracer.noOpTracer[Task, NetworkEvent[ECPublicKey, InterpreterMessage]]
      Tracer.instance[Task, NetworkEvent[ECPublicKey, InterpreterMessage]] {
        event =>
          Task {
            val now = Instant.now()
            println(s"$localAddress - $now: $event")
          }
      }
    )

    for {
      implicit0(scheduler: Scheduler) <- Resource.make(
        Task(Scheduler.io("interpreter-service-test"))
      )(scheduler => Task(scheduler.shutdown()))

      connectionProvider <- ScalanetConnectionProvider[
        Task,
        ECPublicKey,
        InterpreterMessage
      ](
        bindAddress = localAddress,
        nodeKeyPair = localKeyPair,
        new SecureRandom(),
        useNativeTlsImplementation = true,
        framingConfig = DynamicTLSPeerGroup.FramingConfig
          .buildStandardFrameConfig(
            maxFrameLength = 1024 * 1024,
            lengthFieldLength = 8
          )
          .fold(e => sys.error(e.description), identity),
        maxIncomingQueueSizePerPeer = 100
      )

      connectionManager <- LocalConnectionManager[
        Task,
        ECPublicKey,
        InterpreterMessage
      ](
        connectionProvider,
        targetKeyPair.pub,
        targetAddress,
        retryConfig
      )

    } yield connectionManager
  }
}
