package io.iohk.metronome.checkpointing.interpreter

import cats.implicits._
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
import io.iohk.metronome.checkpointing.models.{Block, Ledger, Transaction}
import io.iohk.metronome.checkpointing.models.CheckpointCertificate
import io.iohk.metronome.checkpointing.interpreter.tracing.InterpreterEvent
import io.iohk.metronome.tracer.Tracer
import io.iohk.scalanet.peergroup.dynamictls.DynamicTLSPeerGroup
import java.net.{InetSocketAddress, ServerSocket}
import java.security.SecureRandom
import monix.eval.Task
import monix.execution.Scheduler
import monix.tail.Iterant
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.compatible.Assertion
import scala.concurrent.Future
import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers
import org.scalatest.Inside
import scodec.bits.BitVector

class InterpreterServiceSpec extends AsyncFlatSpec with Matchers with Inside {
  import InterpreterServiceSpec.{Fixture, MockInterpreterRPC}
  import InterpreterMessage._

  def test(fixture: Fixture): Future[Assertion] = {
    import Scheduler.Implicits.global
    fixture.resources.use(fixture.test).timeout(10.seconds).runToFuture
  }

  behavior of "InterpreterService"

  it should "send messages from the Interpreter to the Service" in test {
    new Fixture {
      override def test(input: Input): Task[Assertion] =
        for {
          _ <- input.serviceRpc.newCheckpointCandidate
          pb = Transaction.ProposerBlock(BitVector.fromInt(1))
          _  <- input.serviceRpc.newProposerBlock(pb)
          m1 <- input.nextServiceMessage
          m2 <- input.nextServiceMessage
        } yield {
          m1 shouldBe an[NewCheckpointCandidateRequest]
          inside(m2) { case NewProposerBlockRequest(_, txn) =>
            txn shouldBe pb
          }
        }
    }
  }

  it should "send messages from the Service to the Interpreter" in test {
    new Fixture {
      val mockValidationResult = true

      override lazy val interpreterRpc: InterpreterRPC[Task] =
        new MockInterpreterRPC() {
          override def validateBlockBody(
              blockBody: Block.Body,
              ledger: Ledger
          ) =
            mockValidationResult.some.pure[Task]
        }

      override def test(input: Input): Task[Assertion] =
        for {
          requestId <- RequestId[Task]
          request = ValidateBlockBodyRequest(
            requestId,
            blockBody = Block.Body(transactions = Vector.empty),
            ledger = Ledger.empty
          )
          _        <- input.sendServiceMessage(request)
          response <- input.nextServiceMessage
        } yield {
          inside(response) {
            case ValidateBlockBodyResponse(responseId, isValid) =>
              responseId shouldBe requestId
              isValid shouldBe mockValidationResult
          }
        }
    }
  }
}

object InterpreterServiceSpec {

  val retryConfig = RemoteConnectionManager.RetryConfig.default.copy(
    maxDelay = 5.seconds
  )

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
    class Input(
        val serviceRpc: ServiceRPC[Task],
        serviceConnection: LocalConnectionManager[
          Task,
          ECPublicKey,
          InterpreterMessage
        ],
        serviceConsumer: Iterant.Consumer[Task, InterpreterMessage]
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

      def sendServiceMessage(message: InterpreterMessage): Task[Unit] =
        serviceConnection.sendMessage(message).rethrow
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

        // Wait until the two connection managers are establish 1 connection.
        _ <- Resource.liftF {
          for {
            // Allow some time for connections to be sorted out.
            _ <- Task.sleep(retryConfig.oppositeConnectionOverlap)
            _ <- Task
              .parSequence(
                List(
                  serviceConnection.isConnected.restartUntil(identity),
                  interpreterConnection.isConnected.restartUntil(identity)
                )
              )
              .timeout(5.seconds)
          } yield ()
        }

        // Start processing messages going from the Service to the Interpreter.
        serviceRpc <- InterpreterService[Task](
          interpreterConnection,
          interpreterRpc,
          timeout = 1.second
        )

        serviceConsumer <- serviceConnection.incomingMessages.consume

      } yield new Input(serviceRpc, serviceConnection, serviceConsumer)
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

    implicit val networkTracers = NetworkTracers(
      Tracer.noOpTracer[Task, NetworkEvent[ECPublicKey, InterpreterMessage]]
      // Tracer.instance[Task, NetworkEvent[ECPublicKey, InterpreterMessage]](e =>
      //   Task(println(e))
      // )
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
