package io.iohk.metronome.checkpointing.service

import cats.data.NonEmptyList
import cats.effect.Resource
import cats.effect.concurrent.Ref
import cats.implicits._
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.checkpointing.models.Transaction.CheckpointCandidate
import io.iohk.metronome.checkpointing.models._
import io.iohk.metronome.checkpointing.service.CheckpointingService.{
  LedgerNode,
  LedgerTree
}
import io.iohk.metronome.hotstuff.consensus.basic.Phase.Commit
import io.iohk.metronome.hotstuff.consensus.basic.QuorumCertificate
import monix.eval.Task
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.{all, classify, forAll, forAllNoShrink, propBoolean}
import org.scalacheck.{Gen, Properties}

import scala.util.Random

/** Props for Checkpointing service
  *
  * Do take note of tests that use `classify` to report whether parallelism
  * was achieved. This is not a hard requirement because it may fail on CI,
  * but one should make sure to achieve 100% parallelism locally when making
  * changes to this tests or the service
  */
class CheckpointingServiceProps extends Properties("CheckpointingService") {

  import CheckpointingServiceFixtures._

  case class TestFixture(
      initialBlock: Block,
      initialLedger: Ledger,
      batch: List[Block],
      commitQC: QuorumCertificate[CheckpointingAgreement],
      randomSeed: Long
  ) extends BaseFixture {

    class InterpreterClient(
        val lastCheckpointCertRef: Ref[Task, Option[CheckpointCertificate]]
    ) extends DefaultMockInterpreterClient {

      override def validateBlockBody(
          blockBody: Block.Body,
          ledger: Ledger
      ): Task[Option[Boolean]] = Task.pure(true.some)

      override def newCheckpointCertificate(
          checkpointCertificate: CheckpointCertificate
      ): Task[Unit] =
        lastCheckpointCertRef.set(checkpointCertificate.some)
    }

    override val interpreterClientResource: Resource[Task, InterpreterClient] =
      Resource.liftF {
        Ref[Task]
          .of(None: Option[CheckpointCertificate])
          .map(new InterpreterClient(_))
      }

    // not used in the impl so a senseless value
    val commitPath = NonEmptyList.one(initialBlock.header.parentHash)

    lazy val allTransactions = batch.flatMap(_.body.transactions)
    lazy val finalLedger =
      initialLedger.update(batch.flatMap(_.body.transactions))

    lazy val expectedCheckpointCert = allTransactions.reverse.collectFirst {
      case candidate: CheckpointCandidate =>
        //apparently identical checkpoints can be generated in different blocks
        val blockPath = batch.drop(
          batch.lastIndexWhere(_.body.transactions.contains(candidate))
        )
        val headerPath = NonEmptyList.fromListUnsafe(blockPath.map(_.header))

        CheckpointCertificate.construct(blockPath.head, headerPath, commitQC)
    }.flatten
  }

  object TestFixture {
    import ArbitraryInstances._

    def gen(minChain: Int = 1): Gen[TestFixture] = {
      for {
        block <- arbitrary[Block]
        ledger = Ledger.empty.update(block.body.transactions)
        batch    <- genBlockChain(block, ledger, min = minChain, max = 5)
        commitQC <- genQC(Commit, batch.last.hash)
        seed     <- Gen.posNum[Long]
      } yield TestFixture(block, ledger, batch, commitQC, seed)
    }

  }

  // TODO: PM-3107 verify that mempool was filtered
  property("normal execution") = forAll(TestFixture.gen()) { fixture =>
    fixture.run { res =>
      import fixture._
      import res._

      val execution = batch
        .map(checkpointingService.executeBlock(_, commitQC, commitPath))
        .sequence

      val ledgerStorageCheck = store.runReadOnly {
        ledgerStorage.get(finalLedger.hash)
      }

      for {
        results         <- execution
        persistedLedger <- ledgerStorageCheck
        ledgerTree      <- ledgerTreeRef.get
        lastCheckpoint  <- interpreterClient.lastCheckpointCertRef.get
        checkpointData  <- checkpointDataRef.get
      } yield {
        val ledgerTreeUpdated =
          ledgerTree == LedgerTree.root(finalLedger, batch.last.header)

        val executionSuccessful = results.reverse match {
          case true :: rest => !rest.exists(identity)
          case _            => false
        }

        all(
          "execution successful" |: executionSuccessful,
          "ledger persisted" |: persistedLedger.contains(finalLedger),
          "ledgerTree updated" |: ledgerTreeUpdated,
          "checkpoint constructed correctly" |: lastCheckpoint == expectedCheckpointCert,
          "checkpoint data cleared" |: checkpointData.isEmpty
        )
      }
    }
  }

  property("interrupted execution") = forAll(TestFixture.gen(minChain = 2)) {
    fixture =>
      fixture.run { res =>
        import fixture._
        import res._

        // not executing the committed block
        val execution = batch.init
          .map(checkpointingService.executeBlock(_, commitQC, commitPath))
          .sequence

        for {
          results        <- execution
          ledgerTree     <- ledgerTreeRef.get
          lastCheckpoint <- interpreterClient.lastCheckpointCertRef.get
        } yield {
          val ledgerTreeUpdated =
            batch.init.map(_.hash).forall(ledgerTree.contains)
          val executionSuccessful = !results.exists(identity)

          all(
            "executed correctly" |: executionSuccessful,
            "ledgerTree updated" |: ledgerTreeUpdated,
            "checkpoint constructed correctly" |: lastCheckpoint.isEmpty
          )
        }
      }
  }

  property("failed execution - no parent") =
    forAll(TestFixture.gen(minChain = 2)) { fixture =>
      fixture.run { res =>
        import fixture._
        import res._

        // parent block or its state is not saved so this must fail
        val execution = batch.tail
          .map(checkpointingService.executeBlock(_, commitQC, commitPath))
          .sequence

        execution.attempt.map {
          case Left(ex: IllegalStateException) =>
            ex.getMessage.contains("Could not execute block")
          case _ => false
        }
      }
    }

  property("failed execution - height below last executed") =
    forAll(TestFixture.gen(minChain = 2)) { fixture =>
      fixture.run { res =>
        import fixture._
        import res._

        val execution = batch
          .map(checkpointingService.executeBlock(_, commitQC, commitPath))
          .sequence

        // repeated execution must fail because we're trying to execute a block of lower height
        // than the last executed block
        execution >>
          execution.attempt.map {
            case Left(ex: IllegalStateException) =>
              ex.getMessage.contains("Could not execute block")
            case _ => false
          }
      }
    }

  property("parallel validation") = forAll(TestFixture.gen(minChain = 4)) {
    fixture =>
      fixture.run { res =>
        import fixture._
        import res._

        // validation in random order so blocks need to be persisted first
        val persistBlocks = store.runReadWrite {
          batch.map(blockStorage.put).sequence
        }

        def validation(
            validating: Ref[Task, Boolean],
            achievedPar: Ref[Task, Boolean]
        ) =
          Task.parSequence {
            new Random(randomSeed)
              .shuffle(batch)
              .map(b =>
                for {
                  v <- validating.getAndSet(true)
                  _ <- achievedPar.update(_ || v)
                  r <- checkpointingService.validateBlock(b)
                  _ <- validating.set(false)
                } yield r.getOrElse(false)
              )
          }

        for {
          _ <- persistBlocks

          // used to make sure that parallelism was achieved
          validating  <- Ref[Task].of(false)
          achievedPar <- Ref[Task].of(false)

          result     <- validation(validating, achievedPar)
          par        <- achievedPar.get
          ledgerTree <- ledgerTreeRef.get
        } yield {
          val ledgerTreeUpdated = batch.forall(b => ledgerTree.contains(b.hash))

          classify(par, "parallelism achieved") {
            all(
              "validation successful" |: result.forall(identity),
              "ledgerTree updated" |: ledgerTreeUpdated
            )
          }
        }
      }
  }

  property("execution parallel to validation") = forAllNoShrink {
    for {
      f   <- TestFixture.gen(minChain = 4)
      ext <- genBlockChain(f.batch.last, f.finalLedger, min = 4, max = 5)
    } yield (f, f.batch ++ ext)
  } { case (fixture, validationBatch) =>
    fixture.run { res =>
      import fixture._
      import res._

      // validation in random order so blocks need to be persisted first
      val persistBlocks = store.runReadWrite {
        validationBatch.map(blockStorage.put).sequence
      }

      def validation(
          validating: Ref[Task, Boolean],
          executing: Ref[Task, Boolean],
          achievedPar: Ref[Task, Boolean]
      ) = {
        new Random(randomSeed)
          .shuffle(validationBatch)
          .map(b =>
            for {
              _ <- validating.set(true)
              e <- executing.get
              _ <- achievedPar.update(_ || e)
              r <- checkpointingService.validateBlock(b)
              _ <- validating.set(false)
            } yield (r.getOrElse(false), b.header.height)
          )
          .sequence
      }

      def execution(
          validating: Ref[Task, Boolean],
          executing: Ref[Task, Boolean],
          achievedPar: Ref[Task, Boolean]
      ) =
        batch
          .map(b =>
            for {
              _ <- executing.set(true)
              v <- validating.get
              _ <- achievedPar.update(_ || v)
              r <- checkpointingService.executeBlock(b, commitQC, commitPath)
              _ <- executing.set(false)
            } yield r
          )
          .sequence

      val ledgerStorageCheck = store.runReadOnly {
        ledgerStorage.get(finalLedger.hash)
      }

      for {
        _ <- persistBlocks

        // used to make sure that parallelism was achieved
        validating  <- Ref[Task].of(false)
        executing   <- Ref[Task].of(false)
        achievedPar <- Ref[Task].of(false)

        (validationRes, executionRes) <- Task.parZip2(
          validation(validating, executing, achievedPar),
          execution(validating, executing, achievedPar)
        )

        par             <- achievedPar.get
        persistedLedger <- ledgerStorageCheck
        ledgerTree      <- ledgerTreeRef.get
        lastCheckpoint  <- interpreterClient.lastCheckpointCertRef.get
        checkpointData  <- checkpointDataRef.get
      } yield {
        val validationsAfterExec = validationRes.collect {
          case (r, h) if h > batch.last.header.height => r
        }

        val executionSuccessful = executionRes.reverse match {
          case true :: rest => !rest.exists(identity)
          case _            => false
        }

        val ledgerTreeReset = batch.reverse match {
          case committed :: rest =>
            ledgerTree
              .get(committed.hash)
              .contains(LedgerNode(finalLedger, committed.header)) &&
              rest.forall(b => !ledgerTree.contains(b.hash))

          case _ => false
        }

        val validationsSaved =
          validationBatch.diff(batch).forall(b => ledgerTree.contains(b.hash))

        classify(par, "parallelism achieved") {
          all(
            "validation successful" |: validationsAfterExec.forall(identity),
            "execution successful" |: executionSuccessful,
            "ledger persisted" |: persistedLedger.contains(finalLedger),
            "ledgerTree reset" |: ledgerTreeReset,
            "ledgerTree contains validations" |: validationsSaved,
            "checkpoint constructed correctly" |: lastCheckpoint == expectedCheckpointCert,
            "checkpoint data cleared" |: checkpointData.isEmpty
          )
        }
      }
    }
  }

}
