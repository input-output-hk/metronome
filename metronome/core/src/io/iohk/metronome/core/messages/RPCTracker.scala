package io.iohk.metronome.core.messages

import cats.implicits._
import cats.effect.{Concurrent, Timer, Sync}
import cats.effect.concurrent.{Ref, Deferred}
import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

/** `RPCTracker` can be used to register outgoing requests and later
  * match them up with incoming responses, thus turning the two independent
  * messages into a `Kleisli[F, Request, Option[Response]]`, where a `None`
  * result means the request timed out.
  */
class RPCTracker[F[_]: Timer: Concurrent, M](
    deferredMapRef: Ref[F, Map[UUID, RPCTracker.Entry[F, _]]],
    defaultTimeout: FiniteDuration
) {
  import RPCTracker.Entry

  def register[
      Req <: RPCMessageCompanion#Request,
      Res <: RPCMessageCompanion#Response
  ](
      request: Req,
      timeout: FiniteDuration = defaultTimeout
  )(implicit
      ev1: Req <:< M,
      ev2: RPCPair.Aux[Req, Res],
      ct: ClassTag[Res]
  ): F[F[Option[Res]]] = {
    val requestId = request.requestId
    for {
      d <- Deferred[F, Option[Res]]
      e = RPCTracker.Entry(d)
      _ <- deferredMapRef.update(_ + (requestId -> e))
      _ <- Concurrent[F].start {
        Timer[F].sleep(timeout) >> completeWithTimeout(requestId)
      }
    } yield d.get
  }

  def complete[Res <: RPCMessageCompanion#Response](
      response: Res
  )(implicit ev: Res <:< M): F[Boolean] = {
    remove(response.requestId).flatMap {
      case None    => false.pure[F]
      case Some(e) => e.complete(response)
    }
  }

  private def completeWithTimeout(requestId: UUID): F[Unit] =
    remove(requestId).flatMap {
      case None    => ().pure[F]
      case Some(e) => e.timeout
    }

  private def remove(requestId: UUID): F[Option[Entry[F, _]]] =
    deferredMapRef.modify { dm =>
      (dm - requestId, dm.get(requestId))
    }
}

object RPCTracker {
  case class Entry[F[_]: Sync, Res](
      deferred: Deferred[F, Option[Res]]
  )(implicit ct: ClassTag[Res]) {
    def timeout: F[Unit] =
      deferred.complete(None).attempt.void

    def complete[M](response: M): F[Boolean] = {
      response match {
        case expected: Res =>
          deferred.complete(Some(expected)).attempt.map(_.isRight)
        case _ =>
          // Not returning an error because if the message arrived after
          // the timeout we wouldn't know anyway what it was meant to complete.
          false.pure[F]
      }
    }
  }

  def apply[F[_]: Concurrent: Timer, M](
      defaultTimeout: FiniteDuration
  ): F[RPCTracker[F, M]] =
    Ref[F].of(Map.empty[UUID, Entry[F, _]]).map {
      new RPCTracker(_, defaultTimeout)
    }
}
