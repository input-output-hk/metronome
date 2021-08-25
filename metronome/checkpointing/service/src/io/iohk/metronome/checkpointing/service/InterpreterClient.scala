package io.iohk.metronome.checkpointing.service

import cats.implicits._
import cats.effect.{Concurrent, Timer, Resource, Sync}
import io.iohk.metronome.core.messages.{RPCTracker, RPCSupport}
import io.iohk.metronome.checkpointing.interpreter.messages.InterpreterMessage
import io.iohk.metronome.checkpointing.interpreter.{ServiceRPC, InterpreterRPC}
import io.iohk.metronome.checkpointing.interpreter.InterpreterService.InterpreterConnection
import io.iohk.metronome.checkpointing.models.{
  Block,
  Ledger,
  Transaction,
  CheckpointCertificate
}
import io.iohk.metronome.checkpointing.service.tracing.CheckpointingEvent
import io.iohk.metronome.tracer.Tracer
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

/** Mirroring the `InterpreterService`, the `InterpreterClient` presents
  * and RPC facade to the Service so that it can talk to the Interpreter.
  */
object InterpreterClient {
  import InterpreterMessage._
  import CheckpointingEvent._

  private class ClientImpl[F[_]: Sync](
      localConnectionManager: InterpreterConnection[F],
      serviceRpc: ServiceRPC[F],
      rpcTracker: RPCTracker[F, InterpreterMessage]
  )(implicit tracer: Tracer[F, CheckpointingEvent])
      extends RPCSupport[
        F,
        Unit,
        InterpreterMessage,
        InterpreterMessage with Request with FromService,
        InterpreterMessage with Response with FromInterpreter
      ](rpcTracker, InterpreterMessage.RequestId[F])
      with InterpreterRPC[F] {

    protected override val sendRequest =
      (_, req) => sendMessage(req)
    protected override val requestTimeout =
      (_, req) => tracer(InterpreterTimeout(req))
    protected override val responseIgnored =
      (_, req, err) => tracer(InterpreterResponseIgnored(req, err))

    override def createBlockBody(
        ledger: Ledger,
        mempool: Seq[Transaction.ProposerBlock]
    ): F[Option[InterpreterRPC.CreateResult]] =
      sendRequest((), CreateBlockBodyRequest(_, ledger, mempool)).map {
        _.map(r => (r.blockBody, r.purgeFromMempool))
      }

    override def validateBlockBody(
        blockBody: Block.Body,
        ledger: Ledger
    ): F[Option[Boolean]] =
      sendRequest((), ValidateBlockBodyRequest(_, blockBody, ledger)).map {
        _.map(_.isValid)
      }

    override def newCheckpointCertificate(
        checkpointCertificate: CheckpointCertificate
    ): F[Unit] =
      for {
        requestId <- RequestId[F]
        request = NewCheckpointCertificateRequest(
          requestId,
          checkpointCertificate
        )
        _ <- sendCommand(request)
      } yield ()

    private def sendCommand(
        command: InterpreterMessage
          with Request
          with FromService
          with NoResponse
    ): F[Unit] =
      sendMessage(command)

    private def sendMessage(
        message: InterpreterMessage with FromService
    ): F[Unit] =
      localConnectionManager.sendMessage(message).flatMap {
        case Left(_)  => tracer(InterpreterUnavailable(message))
        case Right(_) => ().pure[F]
      }

    def processMessages: F[Unit] =
      localConnectionManager.incomingMessages
        .mapEval[Unit] {
          case m: InterpreterMessage.FromService =>
            // This would be a gross programming error, but let's keep it going and just log it.
            val err = new IllegalArgumentException(
              s"Invalid message from the Interpreter: $m"
            )
            tracer(Error(err))

          case response: Response with FromInterpreter =>
            receiveResponse((), response)

          case NewProposerBlockRequest(_, proposerBlock) =>
            serviceRpc.newProposerBlock(proposerBlock).handleErrorWith {
              case NonFatal(ex) =>
                tracer(Error(ex))
            }

          case NewCheckpointCandidateRequest(_) =>
            serviceRpc.newCheckpointCandidate.handleErrorWith {
              case NonFatal(ex) =>
                tracer(Error(ex))
            }
        }
        .completedL
  }

  /** Start processing Interpreter messages in the background, delegating to the `serviceRpc`,
    * which should hook into the `CheckpointingService`.
    *
    * Returns an `InterpreterRPC` instance the `CheckpointingService` can use to send requests
    * to the PoW side Interpreter.
    *
    * There's a circular dependency there, which can be handled by the `serviceRpc` having access
    * to some data structures it shares with the `CheckpointingService`, e.g. the mempool.
    *
    * The `timeout` controls how long we wait for a response before completing it locally with `None`.
    */
  def apply[F[_]: Concurrent: Timer](
      localConnectionManager: InterpreterConnection[F],
      serviceRpc: ServiceRPC[F],
      timeout: FiniteDuration
  )(implicit
      tracer: Tracer[F, CheckpointingEvent]
  ): Resource[F, InterpreterRPC[F]] =
    for {
      rpcTracker <- Resource.liftF {
        RPCTracker[F, InterpreterMessage](timeout)
      }
      interpreterRpc = new ClientImpl[F](
        localConnectionManager,
        serviceRpc,
        rpcTracker
      )
      _ <- Concurrent[F].background(interpreterRpc.processMessages)
    } yield interpreterRpc
}
