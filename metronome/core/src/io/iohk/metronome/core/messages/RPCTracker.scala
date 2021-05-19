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

  /** Try to complete an outstanding request with a response.
    *
    * Returns `true` if the response was expected, `false` if
    * it wasn't, or already timed out. An error is returned
    * if the response was expected but the there was a type
    * mismatch.
    */
  def complete[Res <: RPCMessageCompanion#Response](
      response: Res
  )(implicit ev: Res <:< M): F[Either[Throwable, Boolean]] = {
    remove(response.requestId).flatMap {
      case None    => false.asRight[Throwable].pure[F]
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

    def complete[M](response: M): F[Either[Throwable, Boolean]] = {
      response match {
        case expected: Res =>
          deferred
            .complete(Some(expected))
            .attempt
            .map(_.isRight.asRight[Throwable])
        case _ =>
          // Wrong type, as evidenced by `ct` not maching `Res`.
          // Returning an error so that this kind of programming error
          // can be highlighted as soon as possible. Note though that
          // if the request already timed out we can't tell if this
          // error would have happened if the response arrived earlier.
          val error = new IllegalArgumentException(
            s"Invalid response type ${response.getClass.getName}; expected ${ct.runtimeClass.getName}"
          )
          deferred.complete(None).attempt.as(error.asLeft[Boolean])
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
