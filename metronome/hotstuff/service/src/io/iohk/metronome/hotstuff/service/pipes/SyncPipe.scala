package io.iohk.metronome.hotstuff.service.pipes

import cats.effect.{Concurrent, ContextShift}
import io.iohk.metronome.core.Pipe
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.{Agreement, Message}
import io.iohk.metronome.hotstuff.service.Status

object SyncPipe {

  sealed trait Request[+A <: Agreement]
  sealed trait Response[+A <: Agreement]

  /** Request the synchronization component to download
    * any missing dependencies up to the High Q.C.,
    * perform any application specific validation,
    * including the block in the `Prepare` message,
    * and persist the blocks up to, but not including
    * the block in the `Prepare` message.
    *
    * This is because the block being prepared is
    * subject to further validation and voting,
    * while the one in the High Q.C. has gathered
    * a quorum from the federation.
    */
  case class PrepareRequest[A <: Agreement](
      sender: A#PKey,
      prepare: Message.Prepare[A]
  ) extends Request[A]

  /** Respond with the outcome of whether the
    * block we're being asked to prepare is
    * valid, according to the application rules.
    */
  case class PrepareResponse[A <: Agreement](
      request: PrepareRequest[A],
      isValid: Boolean
  ) extends Response[A]

  /** Request that the view state is synchronized with the whole federation,
    * including downloading the block and state corresponding to the latest
    * Commit Q.C.
    *
    * The eventual response should contain the new view status to be applied
    * on the protocol state.
    */
  case class StatusRequest(viewNumber: ViewNumber) extends Request[Nothing]

  /** Response with the new status to resume the protocol from, after the
    * state has been synchronized up to the included Commit Q.C.
    */
  case class StatusResponse[A <: Agreement](
      status: Status[A]
  ) extends Response[A]

  def apply[F[_]: Concurrent: ContextShift, A <: Agreement]: F[SyncPipe[F, A]] =
    Pipe[F, SyncPipe.Request[A], SyncPipe.Response[A]]
}
