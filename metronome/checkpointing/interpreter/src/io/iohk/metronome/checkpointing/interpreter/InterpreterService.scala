package io.iohk.metronome.checkpointing.interpreter

import cats.implicits._
import cats.effect.{Concurrent, Timer, Resource}
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.checkpointing.interpreter.messages.InterpreterMessage
import io.iohk.metronome.checkpointing.interpreter.tracing.InterpreterEvent
import io.iohk.metronome.checkpointing.models.Transaction
import io.iohk.metronome.networking.LocalConnectionManager
import io.iohk.metronome.tracer.Tracer
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import io.iohk.metronome.core.messages.RPCPair

/** The `InterpreterService` is to be used on the Interpreter side to
  * manage the behind-the-scenes messaging with the Checkpointing Service.
  */
object InterpreterService {

  type InterpreterConnection[F[_]] =
    LocalConnectionManager[
      F,
      CheckpointingAgreement.PKey,
      InterpreterMessage
    ]

  private class ServiceImpl[F[_]: Concurrent: Timer](
      localConnectionManager: InterpreterConnection[F],
      interpreterRpc: InterpreterRPC[F],
      // Internal timeout to prevent memory leaks. The Checkpointing Service must have
      // its own `RPCTracker` instance with a suitable timeout to handle non-responses.
      timeout: FiniteDuration
  )(implicit tracer: Tracer[F, InterpreterEvent])
      extends ServiceRPC[F] {
    import InterpreterMessage._
    import InterpreterEvent._

    override def newProposerBlock(
        proposerBlock: Transaction.ProposerBlock
    ): F[Unit] =
      notify(NewProposerBlockRequest(_, proposerBlock))

    override def newCheckpointCandidate: F[Unit] =
      notify(NewCheckpointCandidateRequest(_))

    def processMessages: F[Unit] =
      localConnectionManager.incomingMessages
        .mapEval[Unit] {
          case m: InterpreterMessage.FromInterpreter =>
            // This would be a gross programming error, but let's keep it going and just log it.
            val err = new IllegalArgumentException(
              s"Invalid message from the Checkpointing Service: $m"
            )
            tracer(Error(err))

          case req @ CreateBlockBodyRequest(requestId, ledger, mempool) =>
            respondWith(req, interpreterRpc.createBlockBody(ledger, mempool)) {
              case (body, purgeFromMempool) =>
                CreateBlockBodyResponse(requestId, body, purgeFromMempool)
            }

          case req @ ValidateBlockBodyRequest(requestId, blockBody, ledger) =>
            respondWith(
              req,
              interpreterRpc.validateBlockBody(blockBody, ledger)
            ) { isValid =>
              ValidateBlockBodyResponse(requestId, isValid)
            }

          case req @ NewCheckpointCertificateRequest(_, cert) =>
            noResponse(
              req,
              interpreterRpc.newCheckpointCertificate(cert)
            )
        }
        .completedL

    private def respondWith[
        Req <: InterpreterMessage with Request with FromService,
        Res <: InterpreterMessage with Response with FromInterpreter,
        A
    ](request: Req, maybeResult: F[Option[A]])(
        toResponse: A => Res
    )(implicit ev: RPCPair.Aux[Req, Res]): F[Unit] =
      Concurrent[F].start {
        Concurrent[F]
          .race(
            Timer[F].sleep(timeout),
            maybeResult
          )
          .flatMap {
            case Left(_) =>
              tracer(InterpreterTimeout(request))
            case Right(None) =>
              // The response types contain non-optional values.
              // The optional semantics on the Checkpointing Service side are
              // to be achieved using an `RPCTracker`.
              ().pure[F]
            case Right(Some(result)) =>
              sendMessage(toResponse(result))
          }
          .handleErrorWith { case NonFatal(ex) =>
            tracer(InterpreterError(request, ex))
          }
      }.void

    private def noResponse(
        request: InterpreterMessage
          with Request
          with FromService
          with NoResponse,
        command: F[Unit]
    ): F[Unit] =
      Concurrent[F].start {
        command.handleErrorWith { case NonFatal(ex) =>
          tracer(InterpreterError(request, ex))
        }
      }.void

    private def notify(
        f: RequestId => InterpreterMessage with FromInterpreter with NoResponse
    ): F[Unit] =
      for {
        requestId <- RequestId[F]
        message = f(requestId)
        _ <- sendMessage(message)
      } yield ()

    private def sendMessage(
        message: InterpreterMessage with FromInterpreter
    ): F[Unit] =
      localConnectionManager.sendMessage(message).flatMap {
        case Left(_)  => tracer(ServiceUnavailable(message))
        case Right(_) => ().pure[F]
      }
  }

  /** Start processing messages from the Checkpointing Service in the background, delegating to the `interpreterRPC`,
    * which is the domain specific Interpreter implementation in the host system (i.e. the PoW node).
    *
    * Returns a `ServiceRPC` instance that the host system can use to send notifications to the Checkpointing Service.
    */
  def apply[F[_]: Concurrent: Timer](
      localConnectionManager: InterpreterConnection[F],
      interpreterRpc: InterpreterRPC[F],
      timeout: FiniteDuration
  )(implicit tracer: Tracer[F, InterpreterEvent]): Resource[F, ServiceRPC[F]] =
    for {
      serviceRpc <- Resource.pure[F, ServiceImpl[F]] {
        new ServiceImpl[F](
          localConnectionManager,
          interpreterRpc,
          timeout
        )
      }
      _ <- Concurrent[F].background(serviceRpc.processMessages)
    } yield serviceRpc
}
