package io.iohk.metronome.hotstuff.service.sync

import cats.effect.concurrent.{Ref, Semaphore}
import io.iohk.metronome.crypto.GroupSignature
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.{QuorumCertificate, Phase}
import io.iohk.metronome.hotstuff.service.storage.BlockStorageProps
import io.iohk.metronome.storage.InMemoryKVStore
import org.scalacheck.{Properties, Arbitrary, Gen}, Arbitrary.arbitrary
import org.scalacheck.Prop.{all, forAll, propBoolean}
import monix.eval.Task
import monix.execution.schedulers.TestScheduler
import scala.util.Random
import scala.concurrent.duration._

object BlockSynchronizerProps extends Properties("BlockSynchronizer") {
  import BlockStorageProps.{
    TestAgreement,
    TestBlock,
    TestBlockStorage,
    TestKVStore,
    Namespace,
    genNonEmptyBlockTree
  }

  // Insert the prefix three into "persistent" storage,
  // then start multiple concurrent download processes
  // from random federation members pointing at various
  // nodes in the subtree.
  //
  // In the end all synced subtree elements should be
  // persisted and the ephemeral storage left empty.
  // At no point during the process should the persistent
  // storage contain a forest.
  case class TestFixture(
      ancestorTree: List[TestBlock],
      descendantTree: List[TestBlock],
      requests: List[(TestAgreement.PKey, QuorumCertificate[TestAgreement])]
  ) {
    val persistentRef = Ref.unsafe[Task, TestKVStore.Store] {
      TestKVStore.build(ancestorTree)
    }
    val ephemeralRef = Ref.unsafe[Task, TestKVStore.Store](Map.empty)

    val persistentStore = InMemoryKVStore[Task, Namespace](persistentRef)
    val inMemoryStore   = InMemoryKVStore[Task, Namespace](ephemeralRef)

    val blockMap = (ancestorTree ++ descendantTree).map { block =>
      block.id -> block
    }.toMap

    def getBlock(
        from: TestAgreement.PKey,
        blockHash: TestAgreement.Hash
    ): Task[Option[TestAgreement.Block]] = {
      val timeout   = 5000
      val delay     = Random.nextDouble() * 1000
      val isLost    = Random.nextDouble() < 0.1
      val isCorrupt = Random.nextDouble() < 0.1

      if (isLost) {
        Task.pure(None).delayResult(timeout.millis)
      } else {
        val block  = blockMap(blockHash)
        val result = if (isCorrupt) corrupt(block) else block
        Task.pure(Some(result)).delayResult(delay.millis)
      }
    }

    implicit val storeRunner = persistentStore

    val synchronizer = new BlockSynchronizer[Task, Namespace, TestAgreement](
      blockStorage = TestBlockStorage,
      getBlock = getBlock,
      inMemoryStore = inMemoryStore,
      semaphore = makeSemapshore()
    )

    private def makeSemapshore() = {
      import monix.execution.Scheduler.Implicits.global
      Semaphore[Task](1).runSyncUnsafe()
    }

    def corrupt(block: TestBlock)   = block.copy(id = "corrupt")
    def isCorrupt(block: TestBlock) = block.id == "corrupt"
  }
  object TestFixture {

    implicit val arb: Arbitrary[TestFixture] = Arbitrary {
      for {
        ancestorTree <- genNonEmptyBlockTree
        leaf = ancestorTree.last
        descendantTree <- genNonEmptyBlockTree(parentId = leaf.id)

        federationSize <- Gen.choose(1, 10)
        federationKeys = Range(0, federationSize).toVector

        existingPrepares <- Gen.someOf(ancestorTree)
        newPrepares      <- Gen.atLeastOne(descendantTree)

        prepares = (existingPrepares ++ newPrepares).toList
        proposerKeys <- Gen.listOfN(prepares.size, Gen.oneOf(federationKeys))

        requests = (prepares zip proposerKeys).zipWithIndex.map {
          case ((parent, publicKey), idx) =>
            publicKey -> QuorumCertificate[TestAgreement](
              phase = Phase.Prepare,
              viewNumber = ViewNumber(100L + idx),
              blockHash = parent.id,
              signature = GroupSignature(())
            )
        }

      } yield TestFixture(ancestorTree, descendantTree, requests)
    }
  }

  property("persists") = forAll { (fixture: TestFixture) =>
    implicit val scheduler = TestScheduler()

    val test = for {
      fibers <- Task.parTraverse(fixture.requests) { case (publicKey, qc) =>
        fixture.synchronizer.sync(publicKey, qc).start
      }
      _          <- Task.traverse(fibers)(_.join)
      persistent <- fixture.persistentRef.get
      ephemeral  <- fixture.ephemeralRef.get
    } yield {
      all(
        "ephermeral empty" |: ephemeral.isEmpty,
        "persistent contains all" |: fixture.requests.forall { case (_, qc) =>
          persistent(Namespace.Blocks).contains(qc.blockHash)
        },
        "all uncorrupted" |: persistent(Namespace.Blocks).forall {
          case (blockHash, block: TestBlock) =>
            blockHash == block.id && !fixture.isCorrupt(block)
        }
      )
    }

    // Schedule the execution, using a Future so we can check the value.
    val testFuture = test.runToFuture

    // Simulate a long time, which should be enough for all downloads to finish.
    scheduler.tick(1.day)

    testFuture.value.get.get
  }

  property("no forest") = forAll(
    for {
      fixture  <- arbitrary[TestFixture]
      duration <- arbitrary[Int].map(_.seconds)
    } yield (fixture, duration)
  ) { case (fixture: TestFixture, duration: FiniteDuration) =>
    implicit val scheduler = TestScheduler()

    // Schedule the downloads in the background.
    Task
      .parTraverse(fixture.requests) { case (publicKey, qc) =>
        fixture.synchronizer.sync(publicKey, qc).startAndForget
      }
      .runAsyncAndForget

    // Simulate a some random time, which may or may not be enough to finish the downloads.
    scheduler.tick(duration)

    // Check now that there the persistent store has just one tree.
    val test = for {
      persistent <- fixture.persistentRef.get
    } yield {
      persistent(Namespace.Blocks).forall { case (_, block: TestBlock) =>
        block.parentId.isEmpty || persistent(Namespace.Blocks).contains(
          block.parentId
        )
      }
    }

    val testFuture = test.runToFuture

    // Just simulate the immediate tasks.
    scheduler.tick()

    testFuture.value.get.get
  }
}
