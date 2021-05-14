package io.iohk.metronome.checkpointing.service

import cats.data.{NonEmptyList, NonEmptyVector, OptionT}
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource, Sync}
import cats.implicits._
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.checkpointing.models.{
  Block,
  CheckpointCertificate,
  Ledger
}
import io.iohk.metronome.checkpointing.service.CheckpointingService.LedgerTree
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
    ledgerTree: Ref[F, LedgerTree],
    lastExecutedHeader: Ref[F, Block.Header],
    ledgerStorage: LedgerStorage[N],
    blockStorage: BlockStorage[N, CheckpointingAgreement]
)(implicit storeRunner: KVStoreRunner[F, N])
    extends ApplicationService[F, CheckpointingAgreement] {

  override def createBlock(
      highQC: QuorumCertificate[CheckpointingAgreement]
  ): F[Option[Block]] = ???

  override def validateBlock(block: Block): F[Option[Boolean]] = {
    val ledgers = for {
      nextLedger <- OptionT(projectLedger(block))
      tree       <- OptionT.liftF(ledgerTree.get)
      prevLedger <- tree.get(block.header.parentHash).map(_._1).toOptionT[F]
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
      commitQC: QuorumCertificate[CheckpointingAgreement],
      commitPath: NonEmptyList[Block.Hash]
  ): F[Boolean] = {
    require(commitQC.phase == Phase.Commit, "Commit QC required")
    projectLedger(block).flatMap {
      case Some(ledger) =>
        if (block.hash != commitQC.blockHash)
          saveLedger(block.header, ledger).as(true)
        else
          for {
            //TODO: PM-3110:
            // chkpOpt <- constructCheckpoint(ledger, commitQC)
            _ <- saveLedger(block.header, ledger)
            //TODO: PM-3110:
            // _ <- chkpOpt.map(pushCheckpoint).getOrElse(().pure[F])
          } yield true

      case None =>
        Sync[F].raiseError(
          new IllegalStateException(s"Could not execute block: ${block.hash}")
        )
    }
  }

  private def projectLedger(block: Block): F[Option[Ledger]] = {
    (for {
      ledgers    <- ledgerTree.get
      execHeight <- lastExecutedHeader.get.map(_.height)
    } yield {
      def loop(block: Block): OptionT[F, Ledger] = {
        def doUpdate(ledger: Ledger) =
          OptionT.liftF(updateLedgerByBlock(ledger, block))

        ledgers.get(block.header.parentHash) match {
          case Some((oldLedger, _)) =>
            doUpdate(oldLedger)

          case None if block.header.height <= execHeight =>
            OptionT.none

          case None =>
            for {
              parent    <- OptionT(getBlock(block.header.parentHash))
              oldLedger <- loop(parent)
              newLedger <- doUpdate(oldLedger)
            } yield newLedger
        }
      }

      ledgers
        .get(block.hash)
        .map(_._1)
        .toOptionT[F]
        .orElse(loop(block))
        .value
    }).flatten
  }

  private def updateLedgerByBlock(
      oldLedger: Ledger,
      block: Block
  ): F[Ledger] = {
    val newLedger = oldLedger.update(block.body.transactions)
    ledgerTree
      .update { tree =>
        if (tree.contains(block.header.parentHash))
          tree + (block.hash -> (newLedger, block.header))
        else
          tree
      }
      .as(newLedger)
  }

  private def getBlock(hash: Block.Hash): F[Option[Block]] =
    storeRunner.runReadOnly(blockStorage.get(hash))

  private def saveLedger(header: Block.Header, ledger: Ledger): F[Unit] = {
    storeRunner.runReadWrite {
      ledgerStorage.put(ledger)
    } >>
      ledgerTree.update(clearLedgerTree(header, ledger)) >>
      lastExecutedHeader.set(header)
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
        val (higherLevels, currentLevel) = oldTree.partition {
          case (_, (_, hd)) => hd.height > height
        }
        val children = currentLevel.filter { case (_, (_, hd)) =>
          newTree.contains(hd.parentHash)
        }
        loop(higherLevels, newTree ++ children, height + 1)
      }

    loop(
      ledgerTree.filter { case (_, (_, hd)) =>
        hd.height > commitHeader.height
      },
      Map(commitHeader.hash -> (ledger, commitHeader)),
      commitHeader.height + 1
    )
  }

  private def constructCheckpoint(
      ledger: Ledger,
      commitQC: QuorumCertificate[CheckpointingAgreement]
  ): F[Option[CheckpointCertificate]] =
    ??? //TODO: PM-3110

  private def pushCheckpoint(checkpoint: CheckpointCertificate): F[Unit] =
    ??? //TODO: PM-3137

  override def syncState(
      sources: NonEmptyVector[ECPublicKey],
      block: Block
  ): F[Boolean] = ???
}

object CheckpointingService {
  type LedgerTree = Map[Block.Hash, (Ledger, Block.Header)]

  def apply[F[_]: Concurrent, N](
      ledgerStorage: LedgerStorage[N],
      blockStorage: BlockStorage[N, CheckpointingAgreement],
      viewStateStorage: ViewStateStorage[N, CheckpointingAgreement]
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
      ledgerTree      <- Ref.of(Map(block.hash -> (ledger, block.header)))
      lastExec        <- Ref.of(block.header)
    } yield new CheckpointingService[F, N](
      ledgerTree,
      lastExec,
      ledgerStorage,
      blockStorage
    )

    Resource.liftF(service)
  }

}
