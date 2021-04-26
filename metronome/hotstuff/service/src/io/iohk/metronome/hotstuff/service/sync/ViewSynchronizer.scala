package io.iohk.metronome.hotstuff.service.sync

import cats._
import cats.implicits._
import cats.effect.{Timer, Sync}
import io.iohk.metronome.hotstuff.consensus.Federation
import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.hotstuff.service.Status
import io.iohk.metronome.hotstuff.service.tracing.SyncTracers
import scala.concurrent.duration._
import cats.data.NonEmptyVector

/** The job of the `ViewSynchronizer` is to ask the other federation members
  * what their status is and figure out a view number we should be using.
  * This is something we must do after startup, or if we have for some reason
  * fallen out of sync with the rest of the federation.
  */
class ViewSynchronizer[F[_]: Sync: Timer: Parallel, A <: Agreement](
    federation: Federation[A#PKey],
    getStatus: ViewSynchronizer.GetStatus[F, A],
    retryTimeout: FiniteDuration = 5.seconds
)(implicit tracers: SyncTracers[F, A]) {
  import ViewSynchronizer.aggregateStatus

  /** Poll the federation members for the current status until we have gathered
    * enough to make a decision, i.e. we have a quorum.
    *
    * Pick the highest Quorum Certificates from the gathered responses, but be
    * more careful with he view number as these can be disingenuous.
    *
    * Try again until in one round we can gather all statuses from everyone.
    */
  def sync: F[Status[A]] = {
    federation.publicKeys.toVector
      .parTraverse(getStatus)
      .flatMap { maybeStatuses =>
        tracers
          .statusPoll(federation.publicKeys -> maybeStatuses)
          .as(maybeStatuses.flatten)
      }
      .map(NonEmptyVector.fromVector)
      .flatMap {
        case Some(statuses) if statuses.size >= federation.quorumSize =>
          aggregateStatus(statuses).pure[F]

        case _ =>
          // We traced all responses, so we can detect if we're in an endless loop.
          Timer[F].sleep(retryTimeout) >> sync
      }
  }
}

object ViewSynchronizer {

  /** Send a network request to get the status of a replica. */
  type GetStatus[F[_], A <: Agreement] = A#PKey => F[Option[Status[A]]]

  def aggregateStatus[A <: Agreement](
      statuses: NonEmptyVector[Status[A]]
  ): Status[A] =
    Status(
      viewNumber = median(statuses.map(_.viewNumber)),
      prepareQC = statuses.map(_.prepareQC).maximumBy(_.viewNumber),
      commitQC = statuses.map(_.commitQC).maximumBy(_.viewNumber)
    )

  def median[T: Order](xs: NonEmptyVector[T]): T =
    xs.sorted.getUnsafe((xs.size / 2).toInt)
}
