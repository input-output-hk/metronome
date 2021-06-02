package io.iohk.metronome.hotstuff.service.sync

import cats.implicits._
import cats.data.NonEmptyVector
import cats.effect.concurrent.{Ref, Semaphore}
import io.iohk.metronome.crypto.GroupSignature
import io.iohk.metronome.hotstuff.consensus.{
  ViewNumber,
  Federation,
  LeaderSelection
}
import io.iohk.metronome.hotstuff.consensus.basic.{QuorumCertificate, Phase}
import io.iohk.metronome.hotstuff.service.storage.BlockStorageProps
import io.iohk.metronome.storage.InMemoryKVStore
import org.scalacheck.{Properties, Arbitrary, Gen, Prop}, Arbitrary.arbitrary
import org.scalacheck.Prop.{all, forAll, forAllNoShrink, propBoolean}
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

  case class Prob(value: Double) {
    require(value >= 0 && value <= 1)
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
      requests: List[(TestAgreement.PKey, QuorumCertificate[TestAgreement])],
      federation: Federation[TestAgreement.PKey],
      random: Random,
      timeoutProb: Prob,
      corruptProb: Prob
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

    val downloadedRef = Ref.unsafe[Task, Set[TestAgreement.Hash]](Set.empty)

    def getBlock(
        from: TestAgreement.PKey,
        blockHash: TestAgreement.Hash
    ): Task[Option[TestAgreement.Block]] = {
      val timeout   = 5000
      val delay     = random.nextDouble() * 2900 + 100
      val isTimeout = random.nextDouble() < timeoutProb.value
      val isCorrupt = random.nextDouble() < corruptProb.value

      if (isTimeout) {
        Task.pure(None).delayResult(timeout.millis)
      } else {
        val block  = blockMap(blockHash)
        val result = if (isCorrupt) corrupt(block) else block
        Task {
          downloadedRef.update(_ + blockHash)
        }.as(Some(result)).delayResult(delay.millis)
      }
    }

    implicit val storeRunner = persistentStore

    val synchronizer = new BlockSynchronizer[Task, Namespace, TestAgreement](
      publicKey = federation.publicKeys.head,
      federation = federation,
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

    implicit val arb: Arbitrary[TestFixture] = Arbitrary(gen())

    def gen(timeoutProb: Prob = Prob(0.2), corruptProb: Prob = Prob(0.2)) =
      for {
        ancestorTree <- genNonEmptyBlockTree
        leaf = ancestorTree.last
        descendantTree <- genNonEmptyBlockTree(parentId = leaf.id)

        federationSize <- Gen.choose(3, 10)
        federationKeys = Range(0, federationSize).toVector
        federation = Federation(federationKeys)(LeaderSelection.RoundRobin)
          .getOrElse(sys.error("Can't create federation."))

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

        random <- arbitrary[Int].map(seed => new Random(seed))

      } yield TestFixture(
        ancestorTree,
        descendantTree,
        requests,
        federation,
        random,
        timeoutProb,
        corruptProb
      )
  }

  def simulate(duration: FiniteDuration)(test: Task[Prop]): Prop = {
    implicit val scheduler = TestScheduler()
    // Schedule the execution, using a Future so we can check the value.
    val testFuture = test.runToFuture
    // Simulate a time.
    scheduler.tick(duration)
    // Get the completed results.
    testFuture.value.get.get
  }

  property("sync - persist") = forAll { (fixture: TestFixture) =>
    val test = for {
      fibers <- Task.traverse(fixture.requests) { case (publicKey, qc) =>
        fixture.synchronizer.sync(publicKey, qc).start
      }
      _          <- Task.traverse(fibers)(_.join)
      downloaded <- fixture.downloadedRef.get
      persistent <- fixture.persistentRef.get
      ephemeral  <- fixture.ephemeralRef.get
    } yield {
      all(
        "ephemeral empty" |: ephemeral.isEmpty,
        "persistent contains all" |: fixture.requests.forall { case (_, qc) =>
          persistent(Namespace.Blocks).contains(qc.blockHash)
        },
        "all uncorrupted" |: persistent(Namespace.Blocks).forall {
          case (blockHash, block: TestBlock) =>
            blockHash == block.id && !fixture.isCorrupt(block)
        },
        "not download already persisted" |: fixture.ancestorTree.forall {
          block => !downloaded(block.id)
        }
      )
    }
    // Simulate a long time, which should be enough for all downloads to finish.
    simulate(1.day)(test)
  }

  property("sync - no forest") = forAll(
    for {
      fixture  <- TestFixture.gen(timeoutProb = Prob(0))
      duration <- Gen.choose(1, fixture.requests.size).map(_ * 500.millis)
    } yield (fixture, duration)
  ) { case (fixture: TestFixture, duration: FiniteDuration) =>
    implicit val scheduler = TestScheduler()

    // Schedule the downloads in the background.
    Task
      .traverse(fixture.requests) { case (publicKey, qc) =>
        fixture.synchronizer.sync(publicKey, qc).startAndForget
      }
      .runAsyncAndForget

    // Simulate a some random time, which may or may not be enough to finish the downloads.
    scheduler.tick(duration)

    // Check now that the persistent store has just one tree.
    val test = for {
      persistent <- fixture.persistentRef.get
    } yield {
      persistent(Namespace.Blocks).forall { case (_, block: TestBlock) =>
        // Either the block is the Genesis block with an empty parent ID,
        // or it has a parent which has been inserted into the store.
        block.parentId.isEmpty ||
          persistent(Namespace.Blocks).contains(block.parentId)
      }
    }

    val testFuture = test.runToFuture

    // Just simulate the immediate tasks.
    scheduler.tick()

    testFuture.value.get.get
  }

  property("getBlockFromQuorumCertificate") = forAllNoShrink(
    for {
      fixture <- TestFixture
        .gen(timeoutProb = Prob(0), corruptProb = Prob(0))
      sources <- Gen.pick(
        fixture.federation.quorumSize,
        fixture.federation.publicKeys
      )
      // The last request is definitely new.
      qc = fixture.requests.last._2
    } yield (fixture, sources, qc)
  ) { case (fixture, sources, qc) =>
    val test = for {
      block <- fixture.synchronizer
        .getBlockFromQuorumCertificate(
          sources = NonEmptyVector.fromVectorUnsafe(sources.toVector),
          quorumCertificate = qc
        )
        .rethrow
      persistent <- fixture.persistentRef.get
      ephemeral  <- fixture.ephemeralRef.get
    } yield {
      all(
        "downloaded" |: block.id == qc.blockHash,
        "not in ephemeral" |: ephemeral.isEmpty,
        "not in persistent" |:
          !persistent(Namespace.Blocks).contains(qc.blockHash)
      )
    }
    simulate(1.minute)(test)
  }

  property("getBlockFromQuorumCertificate - timeout") = forAllNoShrink(
    for {
      fixture <- TestFixture.gen(timeoutProb = Prob(1))
      request = fixture.requests.last // Use one that isn't persisted yet.
    } yield (fixture, request._1, request._2)
  ) { case (fixture, source, qc) =>
    val test = for {
      result <- fixture.synchronizer
        .getBlockFromQuorumCertificate(
          sources = NonEmptyVector.one(source),
          quorumCertificate = qc
        )
    } yield "fail with the right exception" |: {
      result match {
        case Left(ex: BlockSynchronizer.DownloadFailedException[_]) =>
          true
        case _ =>
          false
      }
    }
    simulate(1.minute)(test)
  }
}
