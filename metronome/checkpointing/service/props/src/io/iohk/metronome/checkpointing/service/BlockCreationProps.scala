package io.iohk.metronome.checkpointing.service

import cats.data.NonEmptyList
import cats.effect.Resource
import cats.effect.concurrent.Ref
import cats.implicits._
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.checkpointing.models.Transaction.ProposerBlock
import io.iohk.metronome.checkpointing.models._
import io.iohk.metronome.checkpointing.service.CheckpointingService.LedgerNode
import io.iohk.metronome.hotstuff.consensus.basic.Phase
import io.iohk.metronome.hotstuff.consensus.basic.QuorumCertificate
import monix.eval.Task
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.{all, forAll, propBoolean}
import org.scalacheck.{Gen, Properties}
import io.iohk.metronome.checkpointing.interpreter.InterpreterRPC

/** Props for CheckpointingService focusing on `createBlock` */
object BlockCreationProps extends Properties("BlockCreation") {

  import ArbitraryInstances._
  import CheckpointingServiceFixtures._

  case class TestFixture(
      chain: List[Block],
      highQC: QuorumCertificate[CheckpointingAgreement, Phase.Prepare],
      createdBody: Option[Block.Body],
      override val initialMempool: Mempool,
      checkpointNotifications: Boolean
  ) extends BaseFixture {

    require(chain.nonEmpty, "non-empty batch of blocks required")

    override val initialBlock: Block = chain.head
    override val initialLedger: Ledger =
      Ledger.empty.update(initialBlock.body.transactions)

    override val config: CheckpointingService.Config = super.config
      .copy(expectCheckpointCandidateNotifications = checkpointNotifications)

    class InterpreterClient(
        val recordedArguments: Ref[Task, Option[(Ledger, Seq[ProposerBlock])]]
    ) extends DefaultMockInterpreterClient {
      override def createBlockBody(
          ledger: Ledger,
          mempool: Seq[ProposerBlock]
      ): Task[Option[InterpreterRPC.CreateResult]] =
        recordedArguments
          .set((ledger, mempool).some)
          .as(createdBody.map(InterpreterRPC.CreateResult(_)))
    }

    override val interpreterClientResource: Resource[Task, InterpreterClient] =
      Resource.liftF {
        Ref[Task]
          .of(None: Option[(Ledger, Seq[ProposerBlock])])
          .map(new InterpreterClient(_))
      }

    def persistChain(res: TestResources[InterpreterClient]): Task[Unit] =
      res.store.runReadWrite {
        chain.map(res.blockStorage.put).sequence
      }.void

    // TODO: PM-3107 filter mempool
    lazy val projectedMempool: Mempool = initialMempool

    val prefinalLedger: Ledger =
      Ledger.empty.update(chain.flatMap(_.body.transactions))

    val finalLedger: Ledger =
      prefinalLedger.update(createdBody.map(_.transactions).getOrElse(Nil))

    val lastBlock: Block = chain.last
  }

  object TestFixture {
    def gen(minChain: Int = 1): Gen[TestFixture] =
      for {
        block       <- arbitrary[Block]
        chain       <- genBlockChain(block, Ledger.empty, min = minChain, max = 5)
        highQC      <- genQC(Phase.Prepare, chain.last.hash)
        createdBody <- arbitrary[Option[Block.Body]]

        hasCheckpointCandidate = createdBody
          .exists(_.transactions.collect {
            case _: Transaction.CheckpointCandidate =>
          }.nonEmpty)

        initialMempool <- arbitrary[Mempool].map(
          _.copy(hasNewCheckpointCandidate = hasCheckpointCandidate).add(
            chain.flatMap(_.body.proposerBlocks) ++
              createdBody.toSeq.flatMap(_.proposerBlocks)
          )
        )

        checkpointNotifications <- arbitrary[Boolean]
      } yield TestFixture(
        chain,
        highQC,
        createdBody,
        initialMempool,
        checkpointNotifications
      )
  }

  property("normal creation") = forAll(TestFixture.gen()) { fixture =>
    fixture.run { res =>
      import fixture._
      import res._

      for {
        _            <- persistChain(res)
        result       <- checkpointingService.createBlock(highQC)
        recordedArgs <- interpreterClient.recordedArguments.get
        ledgerTree   <- ledgerTreeRef.get
        finalMempool <- mempoolRef.get
      } yield {
        val doQueryInterpreter =
          !projectedMempool.isEmpty || !checkpointNotifications

        val expectedBody =
          if (doQueryInterpreter) createdBody
          else Block.Body.empty.some

        val expectedBlock = expectedBody.map { body =>
          Block
            .make(
              lastBlock.header,
              finalLedger.hash,
              body
            )
        }

        val expectedArgs =
          if (!doQueryInterpreter) None
          else (prefinalLedger, projectedMempool.proposerBlocks).some

        val finalLedgerSaved = expectedBlock match {
          case Some(b) =>
            ledgerTree.get(b.hash).contains(LedgerNode(finalLedger, b.header))
          case None => true
        }

        //no execution so proposer blocks are not cleared
        val expectedMempool = initialMempool.clearCheckpointCandidate

        all(
          "expected block created" |: result == expectedBlock,
          "interpreter called with expected args" |: recordedArgs == expectedArgs,
          "ledgerTree updated wrt created body" |: finalLedgerSaved,
          "mempool updated as expected" |: finalMempool == expectedMempool
        )
      }
    }
  }

  property("failed creation - missing parent") =
    forAll(TestFixture.gen(minChain = 2)) { fixture =>
      fixture.run { res =>
        import fixture._
        import res._

        checkpointingService.createBlock(highQC).map {
          _.isEmpty
        }
      }
    }

  property("failed creation - can't project ledger") =
    forAll(TestFixture.gen(minChain = 2)) { fixture =>
      fixture.run { res =>
        import fixture._
        import res._

        val staleQC = highQC.copy[CheckpointingAgreement, Phase.Prepare](
          blockHash = chain.head.hash
        )
        val commitQC = highQC.copy[CheckpointingAgreement, Phase.Commit](
          phase = Phase.Commit
        )

        val execute = chain.tail
          .map(
            checkpointingService
              .executeBlock(_, commitQC, NonEmptyList.one(chain.head.hash))
          )
          .sequence

        execute >>
          checkpointingService.createBlock(staleQC).map {
            _.isEmpty
          }
      }
    }
}
