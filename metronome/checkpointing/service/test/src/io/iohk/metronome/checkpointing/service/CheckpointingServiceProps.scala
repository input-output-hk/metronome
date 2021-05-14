package io.iohk.metronome.checkpointing.service

import cats.data.NonEmptyList
import cats.effect.Resource
import cats.effect.concurrent.Ref
import cats.implicits._
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.checkpointing.models.Block.{Hash, Header}
import io.iohk.metronome.checkpointing.models.{
  ArbitraryInstances,
  Block,
  Ledger
}
import io.iohk.metronome.checkpointing.service.CheckpointingService.LedgerTree
import io.iohk.metronome.checkpointing.service.storage.LedgerStorage
import io.iohk.metronome.checkpointing.service.storage.LedgerStorageProps.{
  neverUsedCodec,
  Namespace => LedgerNamespace
}
import io.iohk.metronome.hotstuff.consensus.basic.Phase.Commit
import io.iohk.metronome.hotstuff.consensus.basic.QuorumCertificate
import io.iohk.metronome.hotstuff.service.storage.BlockStorage
import io.iohk.metronome.hotstuff.service.storage.BlockStorageProps.{
  Namespace => BlockNamespace
}
import io.iohk.metronome.storage.{
  InMemoryKVStore,
  KVCollection,
  KVStoreRunner,
  KVStoreState,
  KVTree
}
import monix.eval.Task
import monix.execution.Scheduler
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.{all, classify, forAll, forAllNoShrink, propBoolean}
import org.scalacheck.{Gen, Prop, Properties}

import scala.concurrent.duration._
import scala.util.Random

class CheckpointingServiceProps extends Properties("CheckpointingService") {

  type Namespace = String

  case class TestResources(
      checkpointingService: CheckpointingService[Task, Namespace],
      ledgerStorage: LedgerStorage[Namespace],
      blockStorage: BlockStorage[Namespace, CheckpointingAgreement],
      store: KVStoreRunner[Task, Namespace],
      ledgerTreeRef: Ref[Task, LedgerTree]
  )

  case class TestFixture(
      initialBlock: Block,
      initialLedger: Ledger,
      batch: List[Block],
      commitQC: QuorumCertificate[CheckpointingAgreement]
  ) {
    val resources: Resource[Task, TestResources] = {
      val ledgerStorage =
        new LedgerStorage[Namespace](
          new KVCollection[Namespace, Ledger.Hash, Ledger](
            LedgerNamespace.Ledgers
          ),
          LedgerNamespace.LedgerMeta,
          maxHistorySize = 10
        )

      val blockStorage = new BlockStorage[Namespace, CheckpointingAgreement](
        new KVCollection[Namespace, Block.Hash, Block](BlockNamespace.Blocks),
        new KVCollection[Namespace, Block.Hash, KVTree.NodeMeta[Hash]](
          BlockNamespace.BlockMetas
        ),
        new KVCollection[Namespace, Block.Hash, Set[Block.Hash]](
          BlockNamespace.BlockToChildren
        )
      )

      implicit val store = InMemoryKVStore[Task, Namespace](
        Ref.unsafe[Task, KVStoreState[Namespace]#Store](Map.empty)
      )

      Resource.liftF {
        for {
          _ <- store.runReadWrite {
            ledgerStorage.put(initialLedger.hash, initialLedger) >>
              blockStorage.put(initialBlock)
          }

          ledgerTree <- Ref.of[Task, LedgerTree](
            Map(initialBlock.hash -> (initialLedger, initialBlock.header))
          )
          lastExec <- Ref.of[Task, Header](initialBlock.header)

          service = new CheckpointingService[Task, Namespace](
            ledgerTree,
            lastExec,
            ledgerStorage,
            blockStorage
          )

        } yield TestResources(
          service,
          ledgerStorage,
          blockStorage,
          store,
          ledgerTree
        )
      }
    }

    // not used in the impl so a senseless value
    val commitPath = NonEmptyList.one(initialBlock.header.parentHash)

    val allTransactions = batch.flatMap(_.body.transactions)
    val finalLedger     = initialLedger.update(allTransactions)
  }

  object TestFixture {
    import ArbitraryInstances._

    def gen(minChain: Int = 1): Gen[TestFixture] = {
      for {
        block <- arbitrary[Block]
        ledger = Ledger.empty.update(block.body.transactions)
        batch    <- genBlockChain(block, ledger, min = minChain)
        commitQC <- genCommitQC(batch.last)
      } yield TestFixture(block, ledger, batch, commitQC)
    }

    def genBlockChain(
        parent: Block,
        initialLedger: Ledger,
        min: Int = 1,
        max: Int = 6
    ): Gen[List[Block]] = {
      for {
        n      <- Gen.choose(min, max)
        blocks <- Gen.listOfN(n, arbitrary[Block])
      } yield {
        def link(
            parent: Block,
            prevLedger: Ledger,
            chain: List[Block]
        ): List[Block] = chain match {
          case b :: bs =>
            val nextLedger = prevLedger.update(b.body.transactions)
            val header = b.header.copy(
              parentHash = parent.hash,
              height = parent.header.height + 1,
              postStateHash = nextLedger.hash
            )
            val linked = Block.makeUnsafe(header, b.body)
            linked :: link(linked, nextLedger, bs)
          case Nil =>
            Nil
        }

        link(parent, initialLedger, blocks)
      }
    }

    def genCommitQC(
        block: Block
    ): Gen[QuorumCertificate[CheckpointingAgreement]] =
      arbitrary[QuorumCertificate[CheckpointingAgreement]].map {
        _.copy[CheckpointingAgreement](phase = Commit, blockHash = block.hash)
      }
  }

  def run(fixture: TestFixture)(test: TestResources => Task[Prop]): Prop = {
    import Scheduler.Implicits.global

    fixture.resources.use(test).runSyncUnsafe(timeout = 5.seconds)
  }

  property("normal execution") = forAll(TestFixture.gen()) { fixture =>
    run(fixture) { res =>
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
      } yield {
        val ledgerTreeUpdated = ledgerTree == Map(
          batch.last.hash -> (finalLedger, batch.last.header)
        )

        all(
          "execution successful" |: results.reduce(_ && _),
          "ledger persisted" |: persistedLedger.contains(finalLedger),
          "ledgerTree updated" |: ledgerTreeUpdated
        )
      }
    }
  }

  property("failed execution - no parent") =
    forAll(TestFixture.gen(minChain = 2)) { fixture =>
      run(fixture) { res =>
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
      run(fixture) { res =>
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

  //TODO: Validate transactions PM-3131/3132
  //      use a mocked interpreter client that always evaluates blocks as valid
  property("parallel validation") = forAll(TestFixture.gen(minChain = 4)) {
    fixture =>
      run(fixture) { res =>
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
            Random
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

  //TODO: Validate transactions PM-3131/3132
  //      use a mocked interpreter client that always evaluates blocks as valid
  property("execution parallel to validation") = forAllNoShrink {
    for {
      f   <- TestFixture.gen(minChain = 4)
      ext <- TestFixture.genBlockChain(f.batch.last, f.finalLedger)
    } yield (f, f.batch ++ ext)
  } { case (fixture, validationBatch) =>
    run(fixture) { res =>
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
      ) =
        Random
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
      } yield {
        val validationsAfterExec = validationRes.collect {
          case (r, h) if h > batch.last.header.height => r
        }

        val ledgerTreeReset = batch.reverse match {
          case committed :: rest =>
            ledgerTree
              .get(committed.hash)
              .contains((finalLedger, committed.header)) &&
              rest.forall(b => !ledgerTree.contains(b.hash))

          case _ => false
        }

        val validationsSaved =
          validationBatch.diff(batch).forall(b => ledgerTree.contains(b.hash))

        classify(par, "parallelism achieved") {
          all(
            "validation successful" |: validationsAfterExec.forall(identity),
            "execution successful" |: executionRes.forall(identity),
            "ledger persisted" |: persistedLedger.contains(finalLedger),
            "ledgerTree reset" |: ledgerTreeReset,
            "ledgerTree contains validations" |: validationsSaved
          )
        }
      }
    }
  }

}
