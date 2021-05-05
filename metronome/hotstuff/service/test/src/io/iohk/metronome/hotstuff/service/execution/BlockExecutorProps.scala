package io.iohk.metronome.hotstuff.service.execution

import cats.implicits._
import cats.effect.Resource
import cats.effect.concurrent.Ref
import cats.data.NonEmptyVector
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.{
  Effect,
  QuorumCertificate,
  Phase
}
import io.iohk.metronome.crypto.GroupSignature
import io.iohk.metronome.hotstuff.service.ApplicationService
import io.iohk.metronome.hotstuff.service.storage.ViewStateStorage
import io.iohk.metronome.hotstuff.service.storage.{
  BlockStorageProps,
  ViewStateStorageCommands
}
import io.iohk.metronome.hotstuff.service.tracing.{
  ConsensusEvent,
  ConsensusTracers
}
import io.iohk.metronome.storage.InMemoryKVStore
import io.iohk.metronome.tracer.Tracer
import monix.eval.Task
import monix.execution.Scheduler
import org.scalacheck.{Properties, Arbitrary, Gen}
import org.scalacheck.Prop, Prop.{forAll, propBoolean}
import scala.concurrent.duration._

object BlockExecutorProps extends Properties("BlockExecutor") {
  import BlockStorageProps.{
    TestAgreement,
    TestBlock,
    TestBlockStorage,
    TestKVStore,
    Namespace,
    genNonEmptyBlockTree
  }
  import ViewStateStorageCommands.neverUsedCodec

  case class TestFixture(
      blocks: List[TestBlock],
      batches: Vector[Effect.ExecuteBlocks[TestAgreement]]
  ) {
    val storeRef = Ref.unsafe[Task, TestKVStore.Store] {
      TestKVStore.build(blocks)
    }
    val eventsRef =
      Ref.unsafe[Task, Vector[ConsensusEvent[TestAgreement]]](Vector.empty)

    val store = InMemoryKVStore[Task, Namespace](storeRef)

    implicit val storeRunner = store

    val eventTracer =
      Tracer.instance[Task, ConsensusEvent[TestAgreement]] { event =>
        eventsRef.update(_ :+ event)
      }

    implicit val consensusTracers = ConsensusTracers(eventTracer)

    val appService = new ApplicationService[Task, TestAgreement] {
      def createBlock(
          highQC: QuorumCertificate[TestAgreement]
      ): Task[Option[TestBlock]]                         = ???
      def validateBlock(block: TestBlock): Task[Boolean] = ???
      def syncState(
          sources: NonEmptyVector[Int],
          block: TestBlock
      ): Task[Unit]                                  = ???
      def executeBlock(block: TestBlock): Task[Unit] = Task.unit
    }

    val resources =
      for {
        viewStateStorage <- Resource.liftF {
          storeRunner.runReadWrite {
            val genesisQC = QuorumCertificate[TestAgreement](
              phase = Phase.Commit,
              viewNumber = ViewNumber(0),
              blockHash = blocks.head.id,
              signature = GroupSignature(())
            )
            val genesisBundle = ViewStateStorage.Bundle.fromGenesisQC(genesisQC)

            ViewStateStorage[Namespace, TestAgreement](
              "view-state",
              genesisBundle
            )
          }
        }
        blockExecutor <- BlockExecutor[Task, Namespace, TestAgreement](
          appService,
          TestBlockStorage,
          viewStateStorage
        )
      } yield (blockExecutor, viewStateStorage)

    val executedBlockHashes =
      eventsRef.get
        .map { events =>
          events.collect { case ConsensusEvent.BlockExecuted(blockHash) =>
            blockHash
          }
        }

    val lastBatchCommitedBlockHash =
      batches.last.quorumCertificate.blockHash

    def awaitBlockExecution(
        blockHash: TestAgreement.Hash
    ): Task[Vector[TestAgreement.Hash]] = {
      executedBlockHashes
        .restartUntil { blockHashes =>
          blockHashes.lastOption.contains(blockHash)
        }
    }
  }

  object TestFixture {
    implicit val arb: Arbitrary[TestFixture] = Arbitrary {
      def loop(
          i: Int,
          tree: List[TestBlock],
          effects: Vector[Effect.ExecuteBlocks[TestAgreement]]
      ): Gen[TestFixture] = {
        if (i == 0) {
          Gen.const(TestFixture(tree, effects))
        } else {
          val extension = for {
            viewNumber <- Gen.posNum[Int].map(ViewNumber(_))
            ancestor = tree.last
            descendantTree <- genNonEmptyBlockTree(parentId = ancestor.id)
            descendant = descendantTree.last
            commitQC = QuorumCertificate[TestAgreement](
              phase = Phase.Commit,
              viewNumber = viewNumber,
              blockHash = descendant.id,
              signature = GroupSignature(())
            )
            effect = Effect.ExecuteBlocks[TestAgreement](
              lastExecutedBlockHash = ancestor.id,
              quorumCertificate = commitQC
            )
          } yield (tree ++ descendantTree, effects :+ effect)

          extension.flatMap { case (tree, effects) =>
            loop(i - 1, tree, effects)
          }
        }
      }

      for {
        prefixTree <- genNonEmptyBlockTree
        i          <- Gen.choose(1, 5)
        fixture    <- loop(i, prefixTree, Vector.empty)
      } yield fixture
    }
  }

  def run(test: Task[Prop]): Prop = {
    import Scheduler.Implicits.global
    test.runSyncUnsafe(timeout = 5.seconds)
  }

  property("executeBlocks - from root") = forAll { (fixture: TestFixture) =>
    run {
      fixture.resources.use { case (blockSychronizer, _) =>
        for {
          _ <- fixture.batches.traverse(blockSychronizer.enqueue)

          executedBlockHashes <- fixture.awaitBlockExecution(
            fixture.lastBatchCommitedBlockHash
          )

          // The genesis was the only block we marked as executed.
          pathFromRoot <- fixture.storeRunner.runReadOnly {
            TestBlockStorage.getPathFromRoot(fixture.lastBatchCommitedBlockHash)
          }

        } yield {
          "executes from the root" |: executedBlockHashes == pathFromRoot.tail
        }
      }
    }
  }

  property("executeBlocks - from last") = forAll { (fixture: TestFixture) =>
    run {
      fixture.resources.use { case (blockSychronizer, viewStateStorage) =>
        val lastBatch             = fixture.batches.last
        val lastExecutedBlockHash = lastBatch.lastExecutedBlockHash
        for {
          _ <- fixture.storeRunner.runReadWrite {
            viewStateStorage.setLastExecutedBlockHash(lastExecutedBlockHash)
          }
          _ <- blockSychronizer.enqueue(lastBatch)

          executedBlockHashes <- fixture.awaitBlockExecution(
            fixture.lastBatchCommitedBlockHash
          )

          // The genesis was the only block we marked as executed.
          pathFromRoot <- fixture.storeRunner.runReadOnly {
            TestBlockStorage.getPathFromAncestor(
              lastExecutedBlockHash,
              fixture.lastBatchCommitedBlockHash
            )
          }

        } yield {
          "executes from the last" |: executedBlockHashes == pathFromRoot.tail
        }
      }
    }
  }
}
