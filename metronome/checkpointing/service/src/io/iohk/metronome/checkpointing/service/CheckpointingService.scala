package io.iohk.metronome.checkpointing.service

import cats.data.{NonEmptyList, NonEmptyVector, OptionT}
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource, Sync}
import cats.implicits._
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.checkpointing.models.Transaction.CheckpointCandidate
import io.iohk.metronome.checkpointing.models.{
  Block,
  CheckpointCertificate,
  Ledger
}
import io.iohk.metronome.checkpointing.service.CheckpointingService.{
  CheckpointData,
  LedgerNode,
  LedgerTree
}
import io.iohk.metronome.checkpointing.service.storage.LedgerStorage
import io.iohk.metronome.crypto.ECPublicKey
import io.iohk.metronome.hotstuff.consensus.basic.{Phase, QuorumCertificate}
import io.iohk.metronome.hotstuff.service.ApplicationService
import io.iohk.metronome.hotstuff.service.storage.{
  BlockStorage,
  ViewStateStorage
}
import io.iohk.metronome.storage.KVStoreRunner

import scala.annotation.tailrec

class CheckpointingService[F[_]: Sync, N](
    ledgerTreeRef: Ref[F, LedgerTree],
    lastCommittedHeaderRef: Ref[F, Block.Header],
    checkpointDataRef: Ref[F, Option[CheckpointData]],
    //TODO: PM-3137, this is used for testing that a certificate was created correctly
    //      replace with proper means of pushing the certificate to the Interpreter
    pushCheckpointFn: CheckpointCertificate => F[Unit],
    ledgerStorage: LedgerStorage[N],
    blockStorage: BlockStorage[N, CheckpointingAgreement]
)(implicit storeRunner: KVStoreRunner[F, N])
    extends ApplicationService[F, CheckpointingAgreement] {

  override def createBlock(
      highQC: QuorumCertificate[CheckpointingAgreement, Phase.Prepare]
  ): F[Option[Block]] = ???

  override def validateBlock(block: Block): F[Option[Boolean]] = {
    val ledgers = for {
      nextLedger <- OptionT(projectLedger(block))
      tree       <- OptionT.liftF(ledgerTreeRef.get)
      prevLedger <- tree.get(block.header.parentHash).map(_.ledger).toOptionT[F]
    } yield (prevLedger, nextLedger)

    ledgers.value.flatMap {
      case Some((prevLedger, nextLedger))
          if nextLedger.hash == block.header.postStateHash =>
        validateTransactions(block.body, prevLedger)

      case _ => false.some.pure[F]
    }
  }

  private def validateTransactions(
      body: Block.Body,
      ledger: Ledger
  ): F[Option[Boolean]] = {
    //TODO: Validate transactions PM-3131/3132
    true.some.pure[F]
  }

  override def executeBlock(
      block: Block,
      commitQC: QuorumCertificate[CheckpointingAgreement, Phase.Commit],
      commitPath: NonEmptyList[Block.Hash]
  ): F[Boolean] = {
    require(commitQC.phase == Phase.Commit, "Commit QC required")
    projectLedger(block).flatMap {
      case Some(ledger) =>
        updateCheckpointData(block).flatMap { checkpointDataOpt =>
          if (block.hash != commitQC.blockHash)
            false.pure[F]
          else {
            val certificateOpt = checkpointDataOpt
              .flatMap { cd =>
                CheckpointCertificate
                  .construct(cd.block, cd.headers.toNonEmptyList, commitQC)
              }
              .toOptionT[F]

            saveLedger(block.header, ledger) >>
              certificateOpt.cataF(().pure[F], pushCheckpoint) >>
              true.pure[F]
          }
        }

      case None =>
        Sync[F].raiseError(
          new IllegalStateException(s"Could not execute block: ${block.hash}")
        )
    }
  }

  /** Computes and saves the intermediate ledgers leading up to and including
    * the one resulting from the `block` transactions, either by looking up
    * already computed ledgers in the `ledgerTree` or fetching ancestor blocks
    * from `blockStorage`.
    * Only descendants of the root of the `ledgerTree` (last committed ledger)
    * will be evaluated
    */
  private def projectLedger(block: Block): F[Option[Ledger]] = {
    (for {
      ledgerTree   <- ledgerTreeRef.get
      commitHeight <- lastCommittedHeaderRef.get.map(_.height)
    } yield {
      def loop(block: Block): OptionT[F, Ledger] = {
        def doUpdate(ledger: Ledger) =
          OptionT.liftF(updateLedgerByBlock(ledger, block))

        ledgerTree.get(block.header.parentHash) match {
          case Some(oldLedger) =>
            doUpdate(oldLedger.ledger)

          case None if block.header.height <= commitHeight =>
            OptionT.none

          case None =>
            for {
              parent    <- OptionT(getBlock(block.header.parentHash))
              oldLedger <- loop(parent)
              newLedger <- doUpdate(oldLedger)
            } yield newLedger
        }
      }

      ledgerTree
        .get(block.hash)
        .map(_.ledger)
        .toOptionT[F]
        .orElse(loop(block))
        .value
    }).flatten
  }

  /** Computes a new ledger from the `block` and saves it in the ledger tree only if
    * a parent state exists.
    *
    * Because we're only adding to the tree no locking around it is necessary
    */
  private def updateLedgerByBlock(
      oldLedger: Ledger,
      block: Block
  ): F[Ledger] = {
    val newLedger = oldLedger.update(block.body.transactions)

    ledgerTreeRef
      .update { tree =>
        if (tree.contains(block.header.parentHash))
          tree + (block.hash -> LedgerNode(newLedger, block.header))
        else
          tree
      }
      .as(newLedger)
  }

  private def updateCheckpointData(
      block: Block
  ): F[Option[CheckpointData]] = {
    val containsCheckpoint = block.body.transactions.exists {
      case _: CheckpointCandidate => true
      case _                      => false
    }

    checkpointDataRef.updateAndGet { cd =>
      if (containsCheckpoint)
        CheckpointData(block).some
      else
        cd.map(_.extend(block.header))
    }
  }

  private def getBlock(hash: Block.Hash): F[Option[Block]] =
    storeRunner.runReadOnly(blockStorage.get(hash))

  private def saveLedger(header: Block.Header, ledger: Ledger): F[Unit] = {
    storeRunner.runReadWrite {
      ledgerStorage.put(ledger)
    } >>
      ledgerTreeRef.update(clearLedgerTree(header, ledger)) >>
      lastCommittedHeaderRef.set(header) >>
      checkpointDataRef.set(None)
  }

  /** Makes the `commitHeader` and the associated 'ledger' the root of the tree,
    * while retaining any descendants of the `commitHeader`
    */
  private def clearLedgerTree(commitHeader: Block.Header, ledger: Ledger)(
      ledgerTree: LedgerTree
  ): LedgerTree = {

    @tailrec
    def loop(
        oldTree: LedgerTree,
        newTree: LedgerTree,
        height: Long
    ): LedgerTree =
      if (oldTree.isEmpty) newTree
      else {
        val (higherLevels, currentLevel) = oldTree.partition { case (_, ln) =>
          ln.height > height
        }
        val children = currentLevel.filter { case (_, ln) =>
          newTree.contains(ln.parentHash)
        }
        loop(higherLevels, newTree ++ children, height + 1)
      }

    loop(
      ledgerTree.filter { case (_, ln) => ln.height > commitHeader.height },
      LedgerTree.root(ledger, commitHeader),
      commitHeader.height + 1
    )
  }

  private def pushCheckpoint(checkpoint: CheckpointCertificate): F[Unit] =
    pushCheckpointFn(checkpoint) //TODO: PM-3137

  override def syncState(
      sources: NonEmptyVector[ECPublicKey],
      block: Block
  ): F[Boolean] = ???
}

object CheckpointingService {

  /** A node in LedgerTree
    *  `parentHash` and `height` are helpful when resetting the tree
    */
  case class LedgerNode(
      ledger: Ledger,
      parentHash: Block.Hash,
      height: Long
  )

  object LedgerNode {
    def apply(ledger: Ledger, header: Block.Header): LedgerNode =
      LedgerNode(ledger, header.parentHash, header.height)
  }

  /** The internal structure used to represent intermediate ledgers resulting
    * from execution and validation
    */
  type LedgerTree = Map[Block.Hash, LedgerNode]

  object LedgerTree {
    def root(ledger: Ledger, header: Block.Header): LedgerTree =
      Map(header.hash -> LedgerNode(ledger, header))
  }

  /** Used to track the most recent checkpoint candidate
    * `block` - last containing a checkpoint candidate
    * `headers` - path from the `block` to the last executed one
    *
    * These values along with Commit QC can be used to construct
    * a `CheckpointCertificate`
    */
  case class CheckpointData(
      block: Block,
      headers: NonEmptyVector[Block.Header]
  ) {
    def extend(header: Block.Header): CheckpointData =
      copy(headers = headers :+ header)
  }

  object CheckpointData {
    def apply(block: Block): CheckpointData =
      CheckpointData(block, NonEmptyVector.of(block.header))
  }

  def apply[F[_]: Concurrent, N](
      ledgerStorage: LedgerStorage[N],
      blockStorage: BlockStorage[N, CheckpointingAgreement],
      viewStateStorage: ViewStateStorage[N, CheckpointingAgreement],
      pushCheckpointFn: CheckpointCertificate => F[Unit]
  )(implicit
      storeRunner: KVStoreRunner[F, N]
  ): Resource[F, CheckpointingService[F, N]] = {
    val lastExecuted: F[(Block, Ledger)] =
      storeRunner.runReadOnly {
        val query = for {
          blockHash <- OptionT.liftF(
            viewStateStorage.getLastExecutedBlockHash
          )
          block <- OptionT(blockStorage.get(blockHash))
          //a genesis (empty) state should be present in LedgerStorage on first run
          ledger <- OptionT(ledgerStorage.get(block.header.postStateHash))
        } yield (block, ledger)
        query.value
      } >>= {
        _.toOptionT[F].getOrElseF {
          Sync[F].raiseError(
            new IllegalStateException("Last executed block/state not found")
          )
        }
      }

    val service = for {
      (block, ledger) <- lastExecuted
      ledgerTree      <- Ref.of(LedgerTree.root(ledger, block.header))
      lastExec        <- Ref.of(block.header)
      checkpointData  <- Ref.of(None: Option[CheckpointData])
    } yield new CheckpointingService[F, N](
      ledgerTree,
      lastExec,
      checkpointData,
      pushCheckpointFn,
      ledgerStorage,
      blockStorage
    )

    Resource.liftF(service)
  }

}
