package io.iohk.metronome.hotstuff.service.sync

import cats._
import cats.implicits._
import cats.effect.{Timer, Sync}
import cats.data.NonEmptyVector
import io.iohk.metronome.core.Validated
import io.iohk.metronome.hotstuff.consensus.{Federation, ViewNumber}
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  Signing,
  QuorumCertificate,
  Phase
}
import io.iohk.metronome.hotstuff.service.Status
import io.iohk.metronome.hotstuff.service.tracing.SyncTracers
import scala.concurrent.duration._
import io.iohk.metronome.hotstuff.consensus.basic.ProtocolError

/** The job of the `ViewSynchronizer` is to ask the other federation members
  * what their status is and figure out a view number we should be using.
  * This is something we must do after startup, or if we have for some reason
  * fallen out of sync with the rest of the federation.
  */
class ViewSynchronizer[F[_]: Sync: Timer: Parallel, A <: Agreement: Signing](
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
      .parTraverse(getAndValidateStatus)
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

  private def getAndValidateStatus(
      from: A#PKey
  ): F[Option[Validated[Status[A]]]] =
    getStatus(from).flatMap {
      case None =>
        none.pure[F]

      case Some(status) =>
        validate(from, status) match {
          case Left(error) =>
            tracers.invalidStatus(status, error).as(none)
          case Right(valid) =>
            valid.some.pure[F]
        }
    }

  private def validate(
      from: A#PKey,
      status: Status[A]
  ): Either[ProtocolError.InvalidQuorumCertificate[A], Validated[Status[A]]] =
    for {
      _ <- validateQC(from, status.prepareQC)(
        checkPhase(Phase.Prepare),
        checkSignature,
        checkVisible(status),
        _.viewNumber >= status.commitQC.viewNumber
      )
      _ <- validateQC(from, status.commitQC)(
        checkPhase(Phase.Commit),
        checkSignature,
        checkVisible(status),
        _.viewNumber <= status.prepareQC.viewNumber
      )
    } yield Validated[Status[A]](status)

  private def checkPhase(phase: Phase)(qc: QuorumCertificate[A]) =
    phase == qc.phase

  private def checkSignature(qc: QuorumCertificate[A]) =
    Signing[A].validate(federation, qc)

  private def checkVisible(status: Status[A])(qc: QuorumCertificate[A]) =
    status.viewNumber >= qc.viewNumber

  private def validateQC(from: A#PKey, qc: QuorumCertificate[A])(
      checks: (QuorumCertificate[A] => Boolean)*
  ) =
    checks.toList.traverse { check =>
      Either.cond(
        check(qc),
        (),
        ProtocolError.InvalidQuorumCertificate(from, qc)
      )
    }
}

object ViewSynchronizer {

  /** Send a network request to get the status of a replica. */
  type GetStatus[F[_], A <: Agreement] = A#PKey => F[Option[Status[A]]]

  def aggregateStatus[A <: Agreement](
      statuses: NonEmptyVector[Status[A]]
  ): Status[A] = {
    val prepareQC = statuses.map(_.prepareQC).maximumBy(_.viewNumber)
    val commitQC  = statuses.map(_.commitQC).maximumBy(_.viewNumber)
    val viewNumber =
      math.max(median(statuses.map(_.viewNumber)), prepareQC.viewNumber)
    Status(
      viewNumber = ViewNumber(viewNumber),
      prepareQC = prepareQC,
      commitQC = commitQC
    )
  }

  def median[T: Order](xs: NonEmptyVector[T]): T =
    xs.sorted.getUnsafe((xs.size / 2).toInt)
}
