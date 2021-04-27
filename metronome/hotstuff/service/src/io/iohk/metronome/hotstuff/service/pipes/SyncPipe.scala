package io.iohk.metronome.hotstuff.service.pipes

import cats.effect.{Concurrent, ContextShift}
import io.iohk.metronome.core.Pipe
import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.hotstuff.consensus.basic.Message

object SyncPipe {

  sealed trait Request[A <: Agreement]
  sealed trait Response[A <: Agreement]

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

  def apply[F[_]: Concurrent: ContextShift, A <: Agreement]: F[SyncPipe[F, A]] =
    Pipe[F, SyncPipe.Request[A], SyncPipe.Response[A]]
}
