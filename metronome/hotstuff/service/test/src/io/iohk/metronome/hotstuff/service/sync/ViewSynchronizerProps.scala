package io.iohk.metronome.hotstuff.service.sync

import cats.effect.concurrent.Ref
import io.iohk.metronome.hotstuff.consensus.{
  Federation,
  LeaderSelection,
  ViewNumber
}
import io.iohk.metronome.hotstuff.consensus.basic.{
  ProtocolStateCommands,
  QuorumCertificate,
  Phase,
  Signing
}
import io.iohk.metronome.hotstuff.service.Status
import io.iohk.metronome.hotstuff.service.tracing.{SyncTracers, SyncEvent}
import io.iohk.metronome.tracer.Tracer
import monix.eval.Task
import monix.execution.schedulers.TestScheduler
import org.scalacheck.{Arbitrary, Properties, Gen}, Arbitrary.arbitrary
import org.scalacheck.Prop.{forAll, forAllNoShrink, propBoolean, all}
import scala.concurrent.duration._
import java.util.concurrent.TimeoutException
import cats.data.NonEmptySeq
import scala.util.Random

object ViewSynchronizerProps extends Properties("ViewSynchronizer") {
  import ProtocolStateCommands.{
    TestAgreement,
    mockSigning,
    mockSigningKey,
    genInitialState,
    genHash
  }

  /** Projected responses in each round from every federation member. */
  type Responses = Vector[Map[TestAgreement.PKey, TestResponse]]

  /** Generate N rounds worth of test responses, during which the synchronizer
    * should find the first quorum, unless there's none in any of the rounds,
    * in which case it will just keep getting timeouts forever.
    */
  case class TestFixture(
      rounds: Int,
      federation: Federation[TestAgreement.PKey],
      responses: Responses
  ) {
    val responseCounterRef =
      Ref.unsafe[Task, Map[TestAgreement.PKey, Int]](
        federation.publicKeys.map(_ -> 0).toMap
      )

    val syncEventsRef =
      Ref.unsafe[Task, Vector[SyncEvent[TestAgreement]]](Vector.empty)

    private val syncEventTracer =
      Tracer.instance[Task, SyncEvent[TestAgreement]] { event =>
        syncEventsRef.update(_ :+ event)
      }

    implicit val syncTracers: SyncTracers[Task, TestAgreement] =
      SyncTracers(syncEventTracer)

    def getStatus(
        publicKey: TestAgreement.PKey
    ): Task[Option[Status[TestAgreement]]] =
      for {
        round <- responseCounterRef.modify { responseCounter =>
          val count = responseCounter(publicKey)
          responseCounter.updated(publicKey, count + 1) -> count
        }
        result =
          if (round >= responses.size) None
          else
            responses(round)(publicKey) match {
              case TestResponse.Timeout               => None
              case TestResponse.InvalidStatus(status) => Some(status)
              case TestResponse.ValidStatus(status)   => Some(status)
            }
      } yield result
  }

  object TestFixture {
    implicit val leaderSelection = LeaderSelection.RoundRobin

    implicit val arb: Arbitrary[TestFixture] = Arbitrary {
      for {
        state <- genInitialState
        federation = Federation(state.federation, state.f).getOrElse(
          sys.error("Invalid federation.")
        )
        byzantineCount <- Gen.choose(0, state.f)
        byzantines = federation.publicKeys.take(byzantineCount).toSet
        rounds <- Gen.posNum[Int]
        genesisQC = state.newViewsHighQC
        responses <- genResponses(rounds, federation, byzantines, genesisQC)
      } yield TestFixture(
        rounds,
        federation,
        responses
      )
    }
  }

  sealed trait TestResponse
  object TestResponse {
    case object Timeout                                     extends TestResponse
    case class ValidStatus(status: Status[TestAgreement])   extends TestResponse
    case class InvalidStatus(status: Status[TestAgreement]) extends TestResponse
  }

  /** Generate a series of hypothetical responses projected from an idealized consensus process. */
  def genResponses(
      rounds: Int,
      federation: Federation[TestAgreement.PKey],
      byzantines: Set[TestAgreement.PKey],
      genesisQC: QuorumCertificate[TestAgreement]
  ): Gen[Responses] = {

    def genPrepareQC(qc: QuorumCertificate[TestAgreement]) =
      for {
        quorumKeys <- Gen
          .pick(federation.quorumSize, federation.publicKeys)
          .map(_.toVector)
        blockHash <- genHash
        viewNumber = qc.viewNumber.next
        partialSigs = quorumKeys.map { publicKey =>
          val signingKey = mockSigningKey(publicKey)
          Signing[TestAgreement].sign(
            signingKey,
            Phase.Prepare,
            viewNumber,
            blockHash
          )
        }
        groupSig = mockSigning.combine(partialSigs)
      } yield QuorumCertificate[TestAgreement](
        Phase.Prepare,
        viewNumber,
        blockHash,
        groupSig
      )

    def genCommitQC(qc: QuorumCertificate[TestAgreement]) =
      for {
        quorumKeys <- Gen
          .pick(federation.quorumSize, federation.publicKeys)
          .map(_.toVector)
        blockHash  = qc.blockHash
        viewNumber = qc.viewNumber
        partialSigs = quorumKeys.map { publicKey =>
          val signingKey = mockSigningKey(publicKey)
          Signing[TestAgreement].sign(
            signingKey,
            Phase.Commit,
            viewNumber,
            blockHash
          )
        }
        groupSig = mockSigning.combine(partialSigs)
      } yield QuorumCertificate[TestAgreement](
        Phase.Commit,
        viewNumber,
        blockHash,
        groupSig
      )

    def genInvalid(status: Status[TestAgreement]) = {
      def delay(invalid: => Status[TestAgreement]) =
        Gen.delay(Gen.const(invalid))
      Gen.oneOf(
        delay(status.copy(viewNumber = status.prepareQC.viewNumber.prev)),
        delay(status.copy(prepareQC = status.commitQC)),
        delay(status.copy(commitQC = status.prepareQC)),
        delay(
          status.copy(commitQC =
            status.commitQC.copy[TestAgreement](signature =
              status.commitQC.signature
                .copy(sig = status.commitQC.signature.sig.map(_ + 1))
            )
          )
        ).filter(_.commitQC.viewNumber > 0)
      )
    }

    def loop(
        round: Int,
        prepareQC: QuorumCertificate[TestAgreement],
        commitQC: QuorumCertificate[TestAgreement],
        accum: Responses
    ): Gen[Responses] =
      if (round == rounds) Gen.const(accum)
      else {
        val keepCommit = Gen.const(commitQC)

        def maybeCommit(qc: QuorumCertificate[TestAgreement]) =
          if (qc.blockHash != commitQC.blockHash) genCommitQC(qc)
          else keepCommit

        val genRound = for {
          nextPrepareQC <- Gen.oneOf(
            Gen.const(prepareQC),
            genPrepareQC(prepareQC)
          )
          nextCommitQC <- Gen.oneOf(
            keepCommit,
            maybeCommit(prepareQC),
            maybeCommit(nextPrepareQC)
          )
          status = Status(ViewNumber(round + 1), nextPrepareQC, nextCommitQC)
          responses <- Gen.sequence[Vector[TestResponse], TestResponse] {
            federation.publicKeys.map { publicKey =>
              if (byzantines.contains(publicKey)) {
                Gen.frequency(
                  3 -> Gen.const(TestResponse.Timeout),
                  2 -> Gen.const(TestResponse.ValidStatus(status)),
                  5 -> genInvalid(status).map(TestResponse.InvalidStatus(_))
                )
              } else {
                Gen.frequency(
                  1 -> TestResponse.Timeout,
                  4 -> TestResponse.ValidStatus(status)
                )
              }
            }
          }
          responseMap = (federation.publicKeys zip responses).toMap
        } yield (nextPrepareQC, nextCommitQC, responseMap)

        genRound.flatMap { case (prepareQC, commitQC, responseMap) =>
          loop(round + 1, prepareQC, commitQC, accum :+ responseMap)
        }
      }

    loop(
      0,
      genesisQC,
      genesisQC.copy[TestAgreement](phase = Phase.Commit),
      Vector.empty
    )
  }

  property("sync") = forAll { (fixture: TestFixture) =>
    implicit val scheduler = TestScheduler()
    import fixture.syncTracers

    val retryTimeout = 5.seconds
    val syncTimeout  = fixture.rounds * retryTimeout * 2
    val synchronizer = new ViewSynchronizer[Task, TestAgreement](
      federation = fixture.federation,
      getStatus = fixture.getStatus,
      retryTimeout = retryTimeout
    )

    val test = for {
      status <- synchronizer.sync.timeout(syncTimeout).attempt
      events <- fixture.syncEventsRef.get

      quorumSize = fixture.federation.quorumSize

      indexOfQuorum = fixture.responses.indexWhere { responseMap =>
        responseMap.values.collect { case TestResponse.ValidStatus(_) =>
        }.size >= quorumSize
      }
      hasQuorum = indexOfQuorum >= 0

      invalidResponseCount = {
        val responses =
          if (hasQuorum) fixture.responses.take(indexOfQuorum + 1)
          else fixture.responses
        responses
          .flatMap(_.values)
          .collect { case TestResponse.InvalidStatus(_) =>
          }
          .size
      }

      invalidEventCount = {
        events.collect { case x: SyncEvent.InvalidStatus[_] =>
        }.size
      }

      pollSizes = events.collect { case SyncEvent.StatusPoll(statuses) =>
        statuses.size
      }

      responseCounter <- fixture.responseCounterRef.get
    } yield {
      val statusProps = status match {
        case Right(status) =>
          "status" |: all(
            "quorum" |: hasQuorum,
            "reports polls each round" |:
              pollSizes.size == indexOfQuorum + 1,
            "stop at the first quorum" |:
              pollSizes.last >= quorumSize &&
              pollSizes.init.forall(_ < quorumSize),
            "reports all invalid" |:
              invalidEventCount == invalidResponseCount
          )

        case Left(ex: TimeoutException) =>
          "timeout" |: all(
            "no quorum" |: !hasQuorum,
            "empty polls" |: pollSizes.forall(_ < quorumSize),
            "keeps polling" |: pollSizes.size >= fixture.rounds,
            "reports all invalid" |: invalidEventCount == invalidResponseCount
          )

        case Left(ex) =>
          ex.getMessage |: false
      }

      all(
        statusProps,
        "poll everyone in each round" |:
          responseCounter.values.toList.distinct.size <= 2 // Some members can get an extra query, down to timing.
      )
    }

    val testFuture = test.runToFuture

    scheduler.tick(syncTimeout)

    testFuture.value.get.get
  }

  property("median") = forAllNoShrink(
    for {
      m   <- arbitrary[Int].map(_.toLong)
      l   <- Gen.posNum[Int]
      h   <- Gen.oneOf(l, l + 1)
      ls  <- Gen.listOfN(l, Gen.posNum[Int].map(m - _))
      hs  <- Gen.listOfN(l, Gen.posNum[Int].map(m + _))
      rnd <- arbitrary[Int].map(new Random(_))
    } yield (m, rnd.shuffle(ls ++ Seq(m) ++ hs))
  ) { case (m, xs) =>
    m == ViewSynchronizer.median(NonEmptySeq.fromSeqUnsafe(xs))
  }

  property("aggregateStatus") = forAllNoShrink(
    for {
      fixture <- arbitrary[TestFixture]
      statuses = fixture.responses.flatMap(_.values).collect {
        case TestResponse.ValidStatus(status) => status
      }
      if (statuses.nonEmpty)
      rnd <- arbitrary[Int].map(new Random(_))
    } yield NonEmptySeq.fromSeqUnsafe(rnd.shuffle(statuses))
  ) { statuses =>
    val status =
      ViewSynchronizer.aggregateStatus(statuses)

    val medianViewNumber = ViewSynchronizer.median(statuses.map(_.viewNumber))

    val maxViewNumber =
      statuses.map(_.viewNumber).toSeq.max

    val maxPrepareQC =
      statuses.find(_.viewNumber == maxViewNumber).get.prepareQC

    val maxCommitQC =
      statuses.find(_.viewNumber == maxViewNumber).get.commitQC

    all(
      "viewNumber" |:
        status.viewNumber ==
        (if (maxPrepareQC.viewNumber > medianViewNumber) maxPrepareQC.viewNumber
         else medianViewNumber),
      "prepareQC" |: status.prepareQC == maxPrepareQC,
      s"commitQC ${status.commitQC} vs ${maxCommitQC}" |: status.commitQC == maxCommitQC
    )
  }
}
