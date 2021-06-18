package io.iohk.metronome.hotstuff.service.execution

import cats.implicits._
import cats.effect.Resource
import cats.effect.concurrent.{Ref, Semaphore}
import cats.data.{NonEmptyVector, NonEmptyList}
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
import org.scalacheck.Prop, Prop.{forAll, propBoolean, all}
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

  case class TestResources(
      blockExecutor: BlockExecutor[Task, Namespace, TestAgreement],
      viewStateStorage: ViewStateStorage[Namespace, TestAgreement],
      executionSemaphore: Semaphore[Task]
  )

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

    val failNextRef    = Ref.unsafe[Task, Boolean](false)
    val isExecutingRef = Ref.unsafe[Task, Boolean](false)

    private def appService(semaphore: Semaphore[Task]) =
      new ApplicationService[Task, TestAgreement] {
        def createBlock(
            highQC: QuorumCertificate[TestAgreement]
        ): Task[Option[TestBlock]] = ???

        def validateBlock(block: TestBlock): Task[Option[Boolean]] = ???

        def syncState(
            sources: NonEmptyVector[Int],
            block: TestBlock
        ): Task[Boolean] = ???

        def executeBlock(
            block: TestBlock,
            commitQC: QuorumCertificate[TestAgreement],
            commitPath: NonEmptyList[TestAgreement.Hash]
        ): Task[Boolean] =
          isExecutingRef
            .set(true)
            .bracket(_ =>
              semaphore.withPermit {
                for {
                  fail <- failNextRef.modify(failNext => (false, failNext))
                  _ <- Task
                    .raiseError(new RuntimeException("The application failed!"))
                    .whenA(fail)
                } yield true
              }
            )(_ => isExecutingRef.set(false))

      }

    val resources: Resource[Task, TestResources] =
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
        semaphore <- Resource.liftF(Semaphore[Task](1))
        blockExecutor <- BlockExecutor[Task, Namespace, TestAgreement](
          appService(semaphore),
          TestBlockStorage,
          viewStateStorage
        )
      } yield TestResources(blockExecutor, viewStateStorage, semaphore)

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
          blockHashes.contains(blockHash)
        }
    }
  }

  object TestFixture {
    implicit val arb: Arbitrary[TestFixture] = Arbitrary(gen())

    /** Create a random number of tree extensions, with each extension
      * covered by a batch that goes from its root to one of its leaves.
      */
    def gen(minBatches: Int = 1, maxBatches: Int = 5): Gen[TestFixture] = {
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
        i          <- Gen.choose(minBatches, maxBatches)
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
      fixture.resources.use { res =>
        for {
          _ <- fixture.batches.traverse(res.blockExecutor.enqueue)

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
      fixture.resources.use { res =>
        val lastBatch             = fixture.batches.last
        val lastExecutedBlockHash = lastBatch.lastExecutedBlockHash
        for {
          _ <- fixture.storeRunner.runReadWrite {
            res.viewStateStorage.setLastExecutedBlockHash(lastExecutedBlockHash)
          }
          _ <- res.blockExecutor.enqueue(lastBatch)

          executedBlockHashes <- fixture.awaitBlockExecution(
            fixture.lastBatchCommitedBlockHash
          )

          pathFromLast <- fixture.storeRunner.runReadOnly {
            TestBlockStorage.getPathFromAncestor(
              lastExecutedBlockHash,
              fixture.lastBatchCommitedBlockHash
            )
          }

        } yield {
          "executes from the last" |: executedBlockHashes == pathFromLast.tail
        }
      }
    }
  }

  property("executeBlocks - from pruned") = forAll { (fixture: TestFixture) =>
    run {
      fixture.resources.use { res =>
        val lastBatch             = fixture.batches.last
        val lastExecutedBlockHash = lastBatch.lastExecutedBlockHash
        for {
          _ <- fixture.storeRunner.runReadWrite {
            TestBlockStorage.pruneNonDescendants(lastExecutedBlockHash)
          }
          _ <- res.blockExecutor.enqueue(lastBatch)

          executedBlockHashes <- fixture.awaitBlockExecution(
            fixture.lastBatchCommitedBlockHash
          )

          // The last executed block should be the new root.
          pathFromRoot <- fixture.storeRunner.runReadOnly {
            TestBlockStorage.getPathFromRoot(fixture.lastBatchCommitedBlockHash)
          }
        } yield {
          all(
            "new root" |: pathFromRoot.head == lastExecutedBlockHash,
            "executes from the last" |: executedBlockHashes == pathFromRoot.tail
          )
        }
      }
    }
  }

  property("executeBlocks - from failed") =
    // Only the next commit batch triggers re-execution, so we need at least 2.
    forAll(TestFixture.gen(minBatches = 2)) { (fixture: TestFixture) =>
      run {
        fixture.resources.use { res =>
          for {
            _      <- fixture.failNextRef.set(true)
            _      <- fixture.batches.traverse(res.blockExecutor.enqueue)
            _      <- fixture.awaitBlockExecution(fixture.lastBatchCommitedBlockHash)
            events <- fixture.eventsRef.get
          } yield {
            1 === events.count {
              case _: ConsensusEvent.Error => true
              case _                       => false
            }
          }
        }
      }
    }

  property("executeBlocks - skipped") =
    // Using 4 batches so the 2nd batch definitely doesn't start with the last executed block,
    // which will be the root initially, and it's distinct from the last batch as well.
    forAll(TestFixture.gen(minBatches = 4)) { (fixture: TestFixture) =>
      run {
        fixture.resources.use { res =>
          val execBatch = fixture.batches.tail.head
          val lastBatch = fixture.batches.last
          for {
            // Make the execution wait until we update the view state.
            _ <- res.executionSemaphore.acquire
            _ <- res.blockExecutor.enqueue(execBatch)

            // Wait until the execution has started before updating the view state
            // so that all the blocks are definitely enqueued already.
            _ <- fixture.isExecutingRef.get.restartUntil(identity)

            // Now skip ahead, like if we did a fast-forward sync.
            _ <- fixture.storeRunner.runReadWrite {
              res.viewStateStorage.setLastExecutedBlockHash(
                lastBatch.lastExecutedBlockHash
              )
            }
            _ <- res.executionSemaphore.release

            // Easiest indicator of everything being finished is to execute the last batch.
            _ <- res.blockExecutor.enqueue(lastBatch)
            _ <- fixture.awaitBlockExecution(
              lastBatch.quorumCertificate.blockHash
            )

            events <- fixture.eventsRef.get
            executedBlockHashes = events.collect {
              case ConsensusEvent.BlockExecuted(blockHash) => blockHash
            }
            skippedBlockHashes = events.collect {
              case ConsensusEvent.ExecutionSkipped(blockHash) => blockHash
            }

            path <- fixture.storeRunner.runReadOnly {
              TestBlockStorage.getPathFromRoot(
                execBatch.quorumCertificate.blockHash
              )
            }
          } yield {
            all(
              // The first block after the root will be executed, only then do we skip the rest.
              "executes the first block" |: executedBlockHashes.head == path.tail.head,
              "skips rest of the blocks" |: skippedBlockHashes == path.drop(2)
            )
          }
        }
      }
    }
}
