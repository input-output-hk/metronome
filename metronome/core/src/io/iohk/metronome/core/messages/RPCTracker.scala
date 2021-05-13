package io.iohk.metronome.core.messages

import cats.implicits._
import cats.effect.{Concurrent, Timer, Sync}
import cats.effect.concurrent.{Ref, Deferred}
import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

/** `RPCTracker` can be used to register outgoing requests and later
  * match them up with incoming responses, thus it facilitates turning
  * the two independent messages into a `Kleisli[F, Request, Option[Response]]`,
  * by a component that has access to the network, where a `None` result means
  * the operation timed out before a response was received.
  *
  * The workflow is:
  * 0. Receive some request parameters in a method.
  * 1. Create a request ID.
  * 2. Create a request with the ID.
  * 3. Register the request with the tracker, hold on to the handle.
  * 4. Send the request over the network.
  * 5. Wait on the handle, eventually returning the optional result to the caller.
  * 6. Pass every response received from the network to the tracker (on the network handler fiber).
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
      // Used by `RPCTracker.Entry.complete` to make sure only the
      // expected response type can complete a request.
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
          // Wrong type, as evidenced by `ct` not maching `Res`.
          // Not returning an error because if the message arrived after
          // the timeout we wouldn't know anyway what it was meant to complete.
          deferred.complete(None).attempt.as(false)
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
