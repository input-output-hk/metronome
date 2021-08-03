package io.iohk.metronome.core.messages

import cats.implicits._
import cats.effect.Sync
import scala.reflect.ClassTag

/** Utility base classes to capture the pattern of interaction with `RPCTracker`.
  * It provides send and receive methods for requests and responses.
  */
object RPCSupport {
  abstract class Remote[
      F[_]: Sync,
      K,
      M,
      Request <: RPCMessageCompanion#Request,
      Response <: RPCMessageCompanion#Response
  ](
      rpcTracker: RPCTracker[F, M],
      requestId: F[RPCMessageCompanion#RequestId]
  )(implicit ev1: Request <:< M, ev2: Response <:< M) {

    protected def sendRequest: (K, Request) => F[Unit]

    protected val requestTimeout: (K, Request) => F[Unit] =
      (_, _) => ().pure[F]
    protected val responseIgnored: (K, Response, Option[Throwable]) => F[Unit] =
      (_, _, _) => ().pure[F]

    /** Send a request to the peer and track the response.
      *
      * Returns `None` if we're not connected or the request times out.
      */
    protected def sendRequest[Req <: Request, Res <: Response](
        to: K,
        mkRequest: RPCMessageCompanion#RequestId => Req
    )(implicit
        ev: RPCPair.Aux[Req, Res],
        ct: ClassTag[Res]
    ): F[Option[Res]] =
      for {
        requestId <- requestId
        request = mkRequest(requestId)
        join     <- rpcTracker.register[Req, Res](request)
        _        <- sendRequest(to, request)
        maybeRes <- join
        _        <- requestTimeout(to, request).whenA(maybeRes.isEmpty)
      } yield maybeRes

    /** Try to complete a request when the response arrives. */
    protected def receiveResponse[Res <: Response](
        from: K,
        response: Res
    ): F[Unit] =
      rpcTracker.complete(response).flatMap {
        case Right(ok) =>
          responseIgnored(from, response, None).whenA(!ok)
        case Left(ex) =>
          responseIgnored(from, response, Some(ex))
      }
  }

  /** Identical to `RPCSupport.Remote` in functionality but without support for remote address. */
  abstract class Local[
      F[_]: Sync,
      M,
      Request <: RPCMessageCompanion#Request,
      Response <: RPCMessageCompanion#Response
  ](
      rpcTracker: RPCTracker[F, M],
      requestId: F[RPCMessageCompanion#RequestId]
  )(implicit ev1: Request <:< M, ev2: Response <:< M) {

    protected def sendRequest: Request => F[Unit]

    protected val requestTimeout: Request => F[Unit] =
      _ => ().pure[F]
    protected val responseIgnored: (Response, Option[Throwable]) => F[Unit] =
      (_, _) => ().pure[F]

    /** Send a request and track the response.
      *
      * Returns `None` if we're not connected or the request times out.
      */
    protected def sendRequest[Req <: Request, Res <: Response](
        mkRequest: RPCMessageCompanion#RequestId => Req
    )(implicit
        ev: RPCPair.Aux[Req, Res],
        ct: ClassTag[Res]
    ): F[Option[Res]] =
      for {
        requestId <- requestId
        request = mkRequest(requestId)
        join     <- rpcTracker.register[Req, Res](request)
        _        <- sendRequest(request)
        maybeRes <- join
        _        <- requestTimeout(request).whenA(maybeRes.isEmpty)
      } yield maybeRes

    /** Try to complete a request when the response arrives. */
    protected def receiveResponse[Res <: Response](
        response: Res
    ): F[Unit] =
      rpcTracker.complete(response).flatMap {
        case Right(ok) =>
          responseIgnored(response, None).whenA(!ok)
        case Left(ex) =>
          responseIgnored(response, Some(ex))
      }
  }
}
