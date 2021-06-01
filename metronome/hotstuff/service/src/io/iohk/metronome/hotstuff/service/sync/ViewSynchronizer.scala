package io.iohk.metronome.hotstuff.service.sync

import cats._
import cats.implicits._
import cats.effect.{Timer, Sync}
import cats.data.{NonEmptySeq, NonEmptyVector}
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
import io.iohk.metronome.hotstuff.consensus.basic.VotingPhase

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
  import ViewSynchronizer.{aggregateStatus, FederationStatus}

  /** Poll the federation members for the current status until we have gathered
    * enough to make a decision, i.e. we have a quorum.
    *
    * Pick the highest Quorum Certificates from the gathered responses, but be
    * more careful with he view number as these can be disingenuous.
    *
    * Try again until in one round we can gather all statuses from everyone.
    */
  def sync: F[ViewSynchronizer.FederationStatus[A]] = {
    federation.publicKeys.toVector
      .parTraverse(getAndValidateStatus)
      .flatMap { maybeStatuses =>
        val statusMap = (federation.publicKeys zip maybeStatuses).collect {
          case (k, Some(s)) => k -> s
        }.toMap

        tracers
          .statusPoll(statusMap)
          .as(statusMap)
      }
      .flatMap {
        case statusMap if statusMap.size >= federation.quorumSize =>
          val statuses = statusMap.values.toList
          val status   = aggregateStatus(NonEmptySeq.fromSeqUnsafe(statuses))

          // Returning everyone who responded so we always have a quorum sized set to talk to.
          val sources =
            NonEmptyVector.fromVectorUnsafe(statusMap.keySet.toVector)

          FederationStatus(status, sources).pure[F]

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
          case Left((error, hint)) =>
            tracers.invalidStatus(status, error, hint).as(none)
          case Right(valid) =>
            valid.some.pure[F]
        }
    }

  private def validate(
      from: A#PKey,
      status: Status[A]
  ): Either[
    (
        ProtocolError.InvalidQuorumCertificate[A],
        ViewSynchronizer.Hint
    ),
    Validated[Status[A]]
  ] =
    for {
      _ <- validateQC(from, status.prepareQC)(
        checkPhase(Phase.Prepare),
        checkSignature,
        checkVisible(status),
        checkPrepareIsAfterCommit(status)
      )
      _ <- validateQC(from, status.commitQC)(
        checkPhase(Phase.Commit),
        checkSignature,
        checkVisible(status)
      )
    } yield Validated[Status[A]](status)

  private def check(cond: Boolean, hint: => String) =
    if (cond) none else hint.some

  private def checkPhase(phase: Phase)(qc: QuorumCertificate[A, _]) =
    check(phase == qc.phase, s"Phase should be $phase.")

  private def checkSignature(qc: QuorumCertificate[A, _]) =
    check(Signing[A].validate(federation, qc), "Invalid signature.")

  private def checkVisible(status: Status[A])(qc: QuorumCertificate[A, _]) =
    check(
      status.viewNumber >= qc.viewNumber,
      "View number of status earlier than Q.C."
    )

  // This could be checked from either Q.C. perspective.
  private def checkPrepareIsAfterCommit(status: Status[A]) =
    (_: QuorumCertificate[A, _]) =>
      check(
        status.prepareQC.viewNumber >= status.commitQC.viewNumber,
        "Prepare Q.C. lower than Commit Q.C."
      )

  private def validateQC[P <: VotingPhase](
      from: A#PKey,
      qc: QuorumCertificate[A, P]
  )(
      checks: (QuorumCertificate[A, _] => Option[String])*
  ) =
    checks.toList.traverse { check =>
      check(qc)
        .map { hint =>
          ProtocolError.InvalidQuorumCertificate(from, qc) -> hint
        }
        .toLeft(())
    }
}

object ViewSynchronizer {

  /** Extra textual description for errors. */
  type Hint = String

  /** Send a network request to get the status of a replica. */
  type GetStatus[F[_], A <: Agreement] = A#PKey => F[Option[Status[A]]]

  /** Determines the best values to adopt: it picks the highest Prepare and
    * Commit Quorum Certificates, and the median View Number.
    *
    * The former have signatures to prove their validity, but the latter could be
    * gamed by adversarial actors, hence not using the highest value.
    * Multiple rounds of peers trying to sync with each other and picking the
    * median should make them converge in the end, unless an adversarial group
    * actively tries to present different values to every honest node.
    *
    * Another candiate would be to use _mode_.
    */
  def aggregateStatus[A <: Agreement](
      statuses: NonEmptySeq[Status[A]]
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

  /** Pick the middle from an ordered sequence of values.
    *
    * In case of an even number of values, it returns the right
    * one from the two values in the middle, it doesn't take the average.
    *
    * The idea is that we want a value that exists, not something made up,
    * and we prefer the higher value, in case this is a progression where
    * picking the lower one would mean we'd be left behind.
    */
  def median[T: Order](xs: NonEmptySeq[T]): T =
    xs.sorted.getUnsafe(xs.size.toInt / 2)

  /** The final status coupled with the federation members that can serve the data. */
  case class FederationStatus[A <: Agreement](
      status: Status[A],
      sources: NonEmptyVector[A#PKey]
  )
}
