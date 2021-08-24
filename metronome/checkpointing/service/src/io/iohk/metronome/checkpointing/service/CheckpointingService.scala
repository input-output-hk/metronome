package io.iohk.metronome.checkpointing.service

import cats.data.{NonEmptyList, NonEmptyVector, OptionT}
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource, Sync, Timer}
import cats.implicits._
import io.iohk.metronome.core.messages.{RPCSupport, RPCTracker}
import io.iohk.metronome.crypto.ECPublicKey
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.checkpointing.interpreter.InterpreterService.InterpreterConnection
import io.iohk.metronome.checkpointing.interpreter.{InterpreterRPC, ServiceRPC}
import io.iohk.metronome.checkpointing.models.Transaction.CheckpointCandidate
import io.iohk.metronome.checkpointing.models.{
  Block,
  CheckpointCertificate,
  Ledger,
  Mempool,
  Transaction
}
import io.iohk.metronome.checkpointing.service.CheckpointingService.{
  CheckpointData,
  LedgerNode,
  LedgerTree
}
import io.iohk.metronome.checkpointing.service.storage.LedgerStorage
import io.iohk.metronome.checkpointing.service.tracing.CheckpointingEvent
import io.iohk.metronome.checkpointing.service.messages.CheckpointingMessage
import io.iohk.metronome.hotstuff.consensus.basic.{Phase, QuorumCertificate}
import io.iohk.metronome.hotstuff.service.ApplicationService
import io.iohk.metronome.hotstuff.service.storage.{
  BlockStorage,
  ViewStateStorage
}
import io.iohk.metronome.networking.{Network, ConnectionHandler}
import io.iohk.metronome.storage.KVStoreRunner
import io.iohk.metronome.tracer.Tracer
import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

class CheckpointingService[F[_]: Sync, N](
    ledgerTreeRef: Ref[F, LedgerTree],
    mempoolRef: Ref[F, Mempool],
    lastCommittedHeaderRef: Ref[F, Block.Header],
    checkpointDataRef: Ref[F, Option[CheckpointData]],
    ledgerStorage: LedgerStorage[N],
    blockStorage: BlockStorage[N, CheckpointingAgreement],
    interpreterClient: InterpreterRPC[F],
    stateSynchronizer: CheckpointingService.StateSynchronizer[F],
    config: CheckpointingService.Config
)(implicit
    storeRunner: KVStoreRunner[F, N],
    tracer: Tracer[F, CheckpointingEvent]
) extends ApplicationService[F, CheckpointingAgreement] {

  override def createBlock(
      highQC: QuorumCertificate[CheckpointingAgreement, Phase.Prepare]
  ): F[Option[Block]] = (
    for {
      parent    <- OptionT(getBlock(highQC.blockHash))
      oldLedger <- OptionT(projectLedger(parent))
      mempool   <- OptionT.liftF(projectMempool(highQC.blockHash))

      newBody <- OptionT {
        if (mempool.isEmpty && config.expectCheckpointCandidateNotifications)
          Block.Body.empty.some.pure[F]
        else
          interpreterClient.createBlockBody(oldLedger, mempool.proposerBlocks)
      }

      newLedger = oldLedger.update(newBody.transactions)
      newBlock  = Block.make(parent.header, newLedger.hash, newBody)

      _ <- OptionT.liftF(updateLedgerTree(newLedger, newBlock.header))
      _ <- OptionT.liftF(tracer(CheckpointingEvent.Proposing(newBlock)))
    } yield newBlock
  ).value

  override def validateBlock(block: Block): F[Option[Boolean]] = {
    val ledgers = for {
      nextLedger <- OptionT(projectLedger(block))
      tree       <- OptionT.liftF(ledgerTreeRef.get)
      prevLedger <- tree.get(block.header.parentHash).map(_.ledger).toOptionT[F]
    } yield (prevLedger, nextLedger)

    ledgers.value.flatMap {
      case Some((prevLedger, nextLedger))
          if nextLedger.hash == block.header.postStateHash =>
        interpreterClient.validateBlockBody(block.body, prevLedger).flatTap {
          case Some(false) =>
            tracer(CheckpointingEvent.InterpreterValidationFailed(block))
          case _ =>
            ().pure[F]
        }

      case _ =>
        tracer(CheckpointingEvent.StateUnavailable(block)).as(false.some)
    }
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

            tracer(CheckpointingEvent.NewState(ledger)) >>
              saveLedger(block.header, ledger) >>
              certificateOpt.cataF(
                ().pure[F],
                certificate =>
                  tracer(
                    CheckpointingEvent.NewCheckpointCertificate(certificate)
                  ) >>
                    interpreterClient.newCheckpointCertificate(certificate)
              ) >>
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
          OptionT.liftF {
            val newLedger = ledger.update(block.body.transactions)
            updateLedgerTree(newLedger, block.header).as(newLedger)
          }

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

  //TODO: PM-3107
  /** Used when creating a new block, this clears the checkpoint flag
    * and filters the mempool out of any `ProposerBlock` transactions
    * that were included since last committed block up to the
    * `blockHash`
    */
  private def projectMempool(blockHash: Block.Hash): F[Mempool] =
    mempoolRef.getAndUpdate(_.clearCheckpointCandidate)

  /** Saves a new ledger in the tree only if a parent state exists.
    * Because we're only adding to the tree no locking around it is necessary
    */
  private def updateLedgerTree(
      ledger: Ledger,
      header: Block.Header
  ): F[Unit] = {
    ledgerTreeRef
      .update { tree =>
        if (tree.contains(header.parentHash))
          tree + (header.hash -> LedgerNode(ledger, header))
        else
          tree
      }
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

  override def syncState(
      sources: NonEmptyVector[ECPublicKey],
      block: Block
  ): F[Boolean] =
    stateSynchronizer.trySyncState(sources, block.header.postStateHash).rethrow
}

object CheckpointingService {

  case class Config(
      expectCheckpointCandidateNotifications: Boolean,
      interpreterTimeout: FiniteDuration,
      networkTimeout: FiniteDuration
  )

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

  private class ServiceRpcImpl[F[_]](mempoolRef: Ref[F, Mempool])
      extends ServiceRPC[F] {

    override def newProposerBlock(
        proposerBlock: Transaction.ProposerBlock
    ): F[Unit] =
      mempoolRef.update(_.add(proposerBlock))

    override def newCheckpointCandidate: F[Unit] =
      mempoolRef.update(_.withNewCheckpointCandidate)
  }

  trait StateSynchronizer[F[_]] {
    def trySyncState(
        sources: NonEmptyVector[CheckpointingAgreement.PKey],
        stateHash: Ledger.Hash
    ): F[Either[Throwable, Boolean]]
  }

  private class NetworkHandler[F[_]: Sync, N](
      publicKey: CheckpointingAgreement.PKey,
      network: Network[F, CheckpointingAgreement.PKey, CheckpointingMessage],
      ledgerStorage: LedgerStorage[N],
      rpcTracker: RPCTracker[F, CheckpointingMessage]
  )(implicit
      storeRunner: KVStoreRunner[F, N],
      tracer: Tracer[F, CheckpointingEvent]
  ) extends RPCSupport[
        F,
        CheckpointingAgreement.PKey,
        CheckpointingMessage,
        CheckpointingMessage with CheckpointingMessage.Request,
        CheckpointingMessage with CheckpointingMessage.Response
      ](rpcTracker, CheckpointingMessage.RequestId[F])
      with StateSynchronizer[F] {

    protected override val sendRequest = (to, req) =>
      network.sendMessage(to, req)

    protected override val requestTimeout = (to, req) =>
      tracer(CheckpointingEvent.NetworkTimeout(to, req))

    protected override val responseIgnored = (to, req, err) =>
      tracer(CheckpointingEvent.NetworkResponseIgnored(to, req, err))

    def processNetworkMessages: F[Unit] =
      network.incomingMessages
        .mapEval[Unit] {
          case ConnectionHandler.MessageReceived(from, message) =>
            processNetworkMessage(from, message)
        }
        .completedL

    def processNetworkMessage(
        from: CheckpointingAgreement.PKey,
        message: CheckpointingMessage
    ): F[Unit] = {
      import CheckpointingMessage._
      val process = message match {
        case response: CheckpointingMessage.Response =>
          receiveResponse(from, response)

        case GetStateRequest(requestId, stateHash) =>
          storeRunner.runReadOnly {
            ledgerStorage.get(stateHash)
          } flatMap {
            case None =>
              ().pure[F]
            case Some(ledger) =>
              network.sendMessage(from, GetStateResponse(requestId, ledger))
          }
      }

      process.handleErrorWith { case NonFatal(ex) =>
        tracer(CheckpointingEvent.Error(ex))
      }
    }

    /** Try to download the ledger from one of the sources. Return an error
      * if all sources timed out and we couldn't get a response from any of them.
      */
    override def trySyncState(
        sources: NonEmptyVector[CheckpointingAgreement.PKey],
        stateHash: Ledger.Hash
    ): F[Either[Throwable, Boolean]] = {
      import CheckpointingMessage._

      def loop(
          sources: List[CheckpointingAgreement.PKey]
      ): F[Option[Ledger]] = {
        sources match {
          case Nil =>
            none.pure[F]

          case source :: sources =>
            sendRequest(source, GetStateRequest(_, stateHash)) flatMap {
              case Some(response) if response.state.hash == stateHash =>
                response.state.some.pure[F]
              case _ =>
                loop(sources)
            }
        }
      }

      storeRunner.runReadOnly {
        ledgerStorage.get(stateHash)
      } flatMap {
        case None =>
          loop(sources.toList.filterNot(_ == publicKey)) flatMap {
            case None =>
              new IllegalStateException(
                "Could not get the state from any of the sources."
              ).asLeft.pure[F].widen

            case Some(ledger) =>
              storeRunner
                .runReadWrite {
                  ledgerStorage.put(ledger)
                }
                .as(true.asRight)
          }

        case Some(_) =>
          // We have the ledger, so don't clear the block history, we didn't need to jump.
          false.asRight.pure[F]
      }
    }
  }

  def apply[F[_]: Concurrent: Timer, N](
      publicKey: CheckpointingAgreement.PKey,
      network: Network[F, CheckpointingAgreement.PKey, CheckpointingMessage],
      ledgerStorage: LedgerStorage[N],
      blockStorage: BlockStorage[N, CheckpointingAgreement],
      viewStateStorage: ViewStateStorage[N, CheckpointingAgreement],
      interpreterConnection: InterpreterConnection[F],
      config: CheckpointingService.Config
  )(implicit
      storeRunner: KVStoreRunner[F, N],
      tracer: Tracer[F, CheckpointingEvent]
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

    def mkService(
        mempoolRef: Ref[F, Mempool],
        interpreterClient: InterpreterRPC[F],
        networkHandler: NetworkHandler[F, N]
    ) = for {
      (block, ledger) <- lastExecuted
      ledgerTree      <- Ref.of(LedgerTree.root(ledger, block.header))
      lastExec        <- Ref.of(block.header)
      checkpointData  <- Ref.of(None: Option[CheckpointData])
      service = new CheckpointingService[F, N](
        ledgerTree,
        mempoolRef,
        lastExec,
        checkpointData,
        ledgerStorage,
        blockStorage,
        interpreterClient,
        networkHandler,
        config
      )
    } yield service

    for {
      mempool <- Resource.liftF(Ref.of(Mempool.init))
      serviceRpc = new ServiceRpcImpl(mempool)

      rpcTracker <- Resource.liftF(
        RPCTracker[F, CheckpointingMessage](config.networkTimeout)
      )
      networkHandler = new NetworkHandler(
        publicKey,
        network,
        ledgerStorage,
        rpcTracker
      )

      interpreterClient <- InterpreterClient(
        interpreterConnection,
        serviceRpc,
        config.interpreterTimeout
      )

      service <- Resource.liftF(
        mkService(mempool, interpreterClient, networkHandler)
      )

      _ <- Concurrent[F].background {
        networkHandler.processNetworkMessages
      }
    } yield service
  }

}
