package io.iohk.metronome.checkpointing.service

import cats.implicits._
import cats.effect.Resource
import cats.effect.concurrent.Ref
import io.iohk.metronome.checkpointing.models.{
  Transaction,
  Ledger,
  Block,
  CheckpointCertificate
}
import io.iohk.metronome.checkpointing.models.ArbitraryInstances
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.checkpointing.interpreter.ServiceRPC
import io.iohk.metronome.checkpointing.interpreter.messages.InterpreterMessage
import io.iohk.metronome.checkpointing.interpreter.InterpreterRPC
import io.iohk.metronome.checkpointing.service.tracing.CheckpointingEvent
import io.iohk.metronome.networking.LocalConnectionManager
import io.iohk.metronome.tracer.Tracer
import monix.catnap.ConcurrentQueue
import monix.eval.Task
import monix.execution.schedulers.TestScheduler
import monix.tail.Iterant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.compatible.Assertion
import org.scalatest.Inspectors
import org.scalacheck.Arbitrary
import scala.concurrent.duration._

class InterpreterClientSpec extends AnyFlatSpec with Matchers with Inspectors {
  import InterpreterClientSpec._
  import InterpreterMessage._
  import ArbitraryInstances._

  def sample[T: Arbitrary] =
    implicitly[Arbitrary[T]].arbitrary.sample.get

  def test(fixture: Fixture): Assertion = {
    implicit val scheduler = TestScheduler()

    val fut =
      fixture.resources.use(fixture.test).delayExecution(0.second).runToFuture

    // These tests shouldn't take too long, so just simulate some generous amount of time.
    scheduler.tick(1.minute)
    fut.value.get.get
  }

  behavior of "InterpreterClient"

  it should "delegate requests from the connection to the ServiceRPC" in test {
    new Fixture {
      override def test(input: Input) =
        for {
          _ <- input.connection.enqueueMessage(NewCheckpointCandidateRequest(_))
          npbr <- input.connection.enqueueMessage(
            NewProposerBlockRequest(_, sample[Transaction.ProposerBlock])
          )

          _ <- awaitProcessing()

          mempool <- input.serviceRpc.mempoolRef.get
          hasNewCheckpointCandidate <-
            input.serviceRpc.hasNewCheckpointCandidateRef.get
        } yield {
          mempool.contains(npbr.proposerBlock) shouldBe true
          hasNewCheckpointCandidate shouldBe true
        }
    }
  }

  it should "send requests from the InterpreterRPC to the connection" in test {
    new Fixture {
      override def test(input: Input) =
        for {
          _ <- input.interpreterRpc
            .createBlockBody(
              sample[Ledger],
              sample[List[Transaction.ProposerBlock]]
            )
          _ <- input.interpreterRpc
            .validateBlockBody(sample[Block].body, sample[Ledger])
          _ <- input.interpreterRpc
            .newCheckpointCertificate(
              sample[CheckpointCertificate]
            )
          outgoingMessages <- input.connection.outgoingMessagesRef.get
        } yield {
          outgoingMessages should have size 3
          outgoingMessages(0) shouldBe a[CreateBlockBodyRequest]
          outgoingMessages(1) shouldBe a[ValidateBlockBodyRequest]
          outgoingMessages(2) shouldBe a[NewCheckpointCertificateRequest]
        }
    }
  }

  it should "return None if there is no response from the connection" in test {
    new Fixture {
      override def test(input: Input) =
        for {
          r1 <- input.interpreterRpc
            .createBlockBody(
              sample[Ledger],
              sample[List[Transaction.ProposerBlock]]
            )
          r2 <- input.interpreterRpc
            .validateBlockBody(sample[Block].body, sample[Ledger])

          traces <- input.traceRef.get
        } yield {
          r1 shouldBe empty
          r2 shouldBe empty
          traces.collect { case _: CheckpointingEvent.InterpreterTimeout =>
          }.size shouldBe 2
        }
    }
  }

  it should "return Some if there is a response from the connection" in test {
    new Fixture {
      override def test(input: Input) =
        for {
          w1 <- input.interpreterRpc
            .createBlockBody(
              sample[Ledger],
              sample[List[Transaction.ProposerBlock]]
            )
            .start

          w2 <- input.interpreterRpc
            .validateBlockBody(sample[Block].body, sample[Ledger])
            .start

          // Allow some time for outgoing messages to be added.
          _ <- awaitProcessing()

          sb = sample[Block].body
          iv = true
          _ <- input.connection.processOutgoingMessages {
            case CreateBlockBodyRequest(requestId, _, _) =>
              CreateBlockBodyResponse(requestId, sb)
            case ValidateBlockBodyRequest(requestId, _, _) =>
              ValidateBlockBodyResponse(requestId, iv)
          }

          // Allow time for incoming messages to be processed
          _ <- awaitProcessing()

          r1 <- w1.join
          r2 <- w2.join
        } yield {
          r1 shouldBe Some(sb)
          r2 shouldBe Some(iv)
        }
    }
  }

  it should "trace errors calling the ServiceRPC and keep processing" in test {
    new Fixture {

      override def makeServiceRPC(
          mempoolRef: Ref[Task, Mempool],
          hasNewCheckpointCandidateRef: Ref[Task, Boolean]
      ) =
        new MockServiceRPC(mempoolRef, hasNewCheckpointCandidateRef) {
          override def newProposerBlock(
              proposerBlock: Transaction.ProposerBlock
          ): Task[Unit] = Task.raiseError(new RuntimeException("Boom!"))
        }

      override def test(input: Input) =
        for {
          _ <- input.connection.enqueueMessage(
            NewProposerBlockRequest(_, sample[Transaction.ProposerBlock])
          )
          _ <- input.connection.enqueueMessage(
            NewCheckpointCandidateRequest(_)
          )

          _ <- awaitProcessing()

          hasNewCheckpointCandidate <-
            input.serviceRpc.hasNewCheckpointCandidateRef.get
          traces <- input.traceRef.get
        } yield {
          hasNewCheckpointCandidate shouldBe true

          val errors = traces.collect { case e: CheckpointingEvent.Error => e }
          errors.size shouldBe 1
          errors.head.error.getMessage shouldBe "Boom!"
        }
    }
  }
}

object InterpreterClientSpec {
  import InterpreterMessage._

  type Mempool    = Vector[Transaction.ProposerBlock]
  type MessageBox = Vector[InterpreterMessage]

  class MockServiceRPC(
      val mempoolRef: Ref[Task, Mempool],
      val hasNewCheckpointCandidateRef: Ref[Task, Boolean]
  ) extends ServiceRPC[Task] {
    override def newProposerBlock(
        proposerBlock: Transaction.ProposerBlock
    ): Task[Unit] =
      mempoolRef.update(_ :+ proposerBlock)

    override def newCheckpointCandidate: Task[Unit] =
      hasNewCheckpointCandidateRef.set(true)
  }

  class MockLocalConnectionManager(
      incomingMessageQueue: ConcurrentQueue[Task, InterpreterMessage],
      val outgoingMessagesRef: Ref[Task, MessageBox]
  ) extends LocalConnectionManager[
        Task,
        CheckpointingAgreement.PKey,
        InterpreterMessage
      ] {

    /** Deliver a message as if it came from the Interpreter. */
    def enqueueMessage(message: InterpreterMessage): Task[Unit] =
      incomingMessageQueue.offer(message)

    def enqueueMessage[T <: InterpreterMessage](
        mkMessage: RequestId => T
    ): Task[T] =
      for {
        message <- RequestId[Task].map(mkMessage)
        _       <- enqueueMessage(message)
      } yield message

    override def isConnected = Task.pure(true)

    override def incomingMessages =
      Iterant.repeatEvalF(incomingMessageQueue.poll)

    /** Send a message to the interpreter. */
    override def sendMessage(message: InterpreterMessage) =
      outgoingMessagesRef.update(_ :+ message).as(Right(()))

    /** Send mock responses to all current messages going to the interpreter. */
    def processOutgoingMessages(
        f: PartialFunction[
          InterpreterMessage,
          InterpreterMessage with FromInterpreter with Response
        ]
    ): Task[Unit] =
      for {
        requests <- outgoingMessagesRef.get.map(_.toList)
        _        <- outgoingMessagesRef.set(Vector.empty)
        responses = requests.collect(f)
        _ <- responses.traverse(enqueueMessage)
      } yield ()
  }

  abstract class Fixture {
    case class Input(
        interpreterRpc: InterpreterRPC[Task],
        serviceRpc: MockServiceRPC,
        connection: MockLocalConnectionManager,
        traceRef: Ref[Task, Vector[CheckpointingEvent]]
    )

    def test(input: Input): Task[Assertion]

    // Processing happens in the background, so there needs to be some time
    // elapsed between enqueueing a message and asserting the results.
    def awaitProcessing(t: FiniteDuration = 1.second) = Task.sleep(t)

    val resources: Resource[Task, Input] =
      for {
        serviceRpc <- Resource.liftF {
          for {
            mempoolRef <- Ref.of[Task, Vector[Transaction.ProposerBlock]](
              Vector.empty
            )
            hasNewCheckpointCandidateRef <- Ref.of[Task, Boolean](false)
          } yield makeServiceRPC(
            mempoolRef,
            hasNewCheckpointCandidateRef
          )
        }

        connection <- Resource.liftF {
          for {
            incomingMessageQueue <- ConcurrentQueue
              .unbounded[Task, InterpreterMessage](None)
            outgoingMessagesRef <- Ref.of[Task, Vector[InterpreterMessage]](
              Vector.empty
            )
          } yield makeLocalConnectionManager(
            incomingMessageQueue,
            outgoingMessagesRef
          )
        }

        traceRef <- Resource.liftF {
          Ref[Task].of(Vector.empty[CheckpointingEvent])
        }
        implicit0(tracer: Tracer[Task, CheckpointingEvent]) = Tracer
          .instance[Task, CheckpointingEvent] { e =>
            traceRef.update(_ :+ e)
          }

        interpreterRpc <- InterpreterClient[Task](
          connection,
          serviceRpc,
          timeout = 5.seconds
        )
      } yield new Input(interpreterRpc, serviceRpc, connection, traceRef)

    def makeLocalConnectionManager(
        incomingMessageQueue: ConcurrentQueue[Task, InterpreterMessage],
        outgoingMessagesRef: Ref[Task, MessageBox]
    ) = new MockLocalConnectionManager(
      incomingMessageQueue,
      outgoingMessagesRef
    )

    def makeServiceRPC(
        mempoolRef: Ref[Task, Mempool],
        hasNewCheckpointCandidateRef: Ref[Task, Boolean]
    ) = new MockServiceRPC(
      mempoolRef,
      hasNewCheckpointCandidateRef
    )
  }
}
