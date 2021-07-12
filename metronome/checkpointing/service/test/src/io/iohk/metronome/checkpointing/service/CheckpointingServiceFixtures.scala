package io.iohk.metronome.checkpointing.service

import cats.effect.Resource
import cats.effect.concurrent.Ref
import cats.implicits._
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.checkpointing.interpreter.InterpreterRPC
import io.iohk.metronome.checkpointing.models.ArbitraryInstances._
import io.iohk.metronome.checkpointing.models.Block.{Hash, Header}
import io.iohk.metronome.checkpointing.models._
import io.iohk.metronome.checkpointing.service.CheckpointingService.{
  CheckpointData,
  LedgerTree
}
import io.iohk.metronome.checkpointing.service.storage.LedgerStorage
import io.iohk.metronome.checkpointing.service.storage.LedgerStorageProps.{
  neverUsedCodec,
  Namespace => LedgerNamespace
}
import io.iohk.metronome.hotstuff.consensus.basic.{
  QuorumCertificate,
  VotingPhase
}
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
import org.scalacheck.{Gen, Prop}

import scala.concurrent.duration.DurationInt

object CheckpointingServiceFixtures {

  type Namespace = String

  case class TestResources[IC <: InterpreterRPC[Task]](
      checkpointingService: CheckpointingService[Task, Namespace],
      interpreterClient: IC,
      ledgerStorage: LedgerStorage[Namespace],
      blockStorage: BlockStorage[Namespace, CheckpointingAgreement],
      store: KVStoreRunner[Task, Namespace],
      ledgerTreeRef: Ref[Task, LedgerTree],
      mempoolRef: Ref[Task, Mempool],
      checkpointDataRef: Ref[Task, Option[CheckpointData]]
  )

  class DefaultMockInterpreterClient() extends InterpreterRPC[Task] {
    override def createBlockBody(
        ledger: Ledger,
        mempool: Seq[Transaction.ProposerBlock]
    ): Task[Option[Block.Body]] =
      Task.pure(None)

    override def validateBlockBody(
        blockBody: Block.Body,
        ledger: Ledger
    ): Task[Option[Boolean]] = Task.pure(true.some)

    override def newCheckpointCertificate(
        checkpointCertificate: CheckpointCertificate
    ): Task[Unit] = Task.unit
  }

  class Storages {
    implicit val store = InMemoryKVStore[Task, Namespace](
      Ref.unsafe[Task, KVStoreState[Namespace]#Store](Map.empty)
    )

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
  }

  trait BaseFixture {
    val initialBlock: Block
    val initialLedger: Ledger
    val initialMempool: Mempool = Mempool.init

    def config: CheckpointingService.Config = CheckpointingService.Config(
      expectCheckpointCandidateNotifications = true,
      interpreterTimeout = 10.seconds
    )

    type InterpreterClient <: InterpreterRPC[Task]

    val interpreterClientResource: Resource[Task, InterpreterClient]

    lazy val resources: Resource[Task, TestResources[InterpreterClient]] = {

      val storages = new Storages
      import storages._

      interpreterClientResource.flatMap { interpreterClient =>
        Resource.liftF {
          for {
            _ <- store.runReadWrite {
              ledgerStorage.put(initialLedger.hash, initialLedger) >>
                blockStorage.put(initialBlock)
            }

            ledgerTree <- Ref.of[Task, LedgerTree](
              LedgerTree.root(initialLedger, initialBlock.header)
            )
            mempool  <- Ref.of[Task, Mempool](initialMempool)
            lastExec <- Ref.of[Task, Header](initialBlock.header)
            chkpData <- Ref.of[Task, Option[CheckpointData]](None)

            service = new CheckpointingService[Task, Namespace](
              ledgerTree,
              mempool,
              lastExec,
              chkpData,
              ledgerStorage,
              blockStorage,
              interpreterClient,
              config
            )

          } yield TestResources(
            service,
            interpreterClient,
            ledgerStorage,
            blockStorage,
            store,
            ledgerTree,
            mempool,
            chkpData
          )
        }
      }
    }

    def run(test: TestResources[InterpreterClient] => Task[Prop]): Prop = {
      import Scheduler.Implicits.global
      resources.use(test).runSyncUnsafe(timeout = 5.seconds)
    }
  }

  def genBlockChain(
      parentBlock: Block,
      parentLedger: Ledger,
      min: Int,
      max: Int
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

      link(parentBlock, parentLedger, blocks)
    }
  }

  def genQC(
      phase: VotingPhase,
      hash: Block.Hash
  ): Gen[QuorumCertificate[CheckpointingAgreement]] =
    arbitrary[QuorumCertificate[CheckpointingAgreement]].map {
      _.copy[CheckpointingAgreement](phase = phase, blockHash = hash)
    }
}
