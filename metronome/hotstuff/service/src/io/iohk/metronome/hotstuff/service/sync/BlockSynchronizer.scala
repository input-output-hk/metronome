package io.iohk.metronome.hotstuff.service.sync

import cats.implicits._
import cats.effect.{Resource, Sync, Timer}
import cats.effect.concurrent.Ref
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  QuorumCertificate,
  Block
}
import io.iohk.metronome.hotstuff.service.storage.BlockStorage
import io.iohk.metronome.storage.{
  KVStoreRunner,
  KVStoreState,
  KVStore,
  KVStoreRead
}
import scala.concurrent.duration._

/** The job of the `BlockSynchronizer` is to procure missing blocks when a `Prepare`
  * message builds on a High Q.C. that we don't have.
  *
  * It will walk backwards, asking for the ancestors until we find one that we already
  * have in persistent storage, then append blocks to the storage in the opposite order.
  *
  * Since the final block has a Quorum Certificate, there's no need to validate the
  * ancestors, assuming an honest majority in the federation. The only validation we
  * need to do is hash checks to make sure we're getting the correct blocks.
  *
  * The synchronizer keeps the tentative blocks in memory until they can be connected
  * to the persistent storage. We assume that we never have to download the block history
  * back until genesis, but rather that the application will always have support for
  * syncing to any given block and its associated state, to catch up after spending
  * a long time offline. Once that happens the block history should be pruneable.
  */
class BlockSynchronizer[F[_]: Sync: Timer, N, A <: Agreement: Block](
    blockStorage: BlockStorage[N, A],
    getBlock: BlockSynchronizer.GetBlock[F, A],
    inMemoryStoreRef: Ref[F, KVStoreState[N]#Store],
    retryTimeout: FiniteDuration = 5.seconds
)(implicit storeRunner: KVStoreRunner[F, N]) {

  // In memory KVStore query compiler.
  // NOTE: Currently not utilising that we're storing the tree structure in memory,
  // it could be replaced with a simple Map.
  val state = new KVStoreState[N]

  /** Download all blocks up to the one included in the Quorum Certificate. */
  def sync(
      sender: A#PKey,
      quorumCertificate: QuorumCertificate[A]
  ): F[Unit] =
    sync(sender, quorumCertificate.blockHash, Nil)

  // We must take care not to insert blocks into storage and risk losing
  // the pointer to them in a restart, hence keeping the unfinished tree
  // in memory until we find a parent we do have in storage, then
  // insert them in the opposite order.
  private def sync(
      sender: A#PKey,
      blockHash: A#Hash,
      path: List[A#Hash]
  ): F[Unit] = {
    storeRunner
      .runReadOnly {
        blockStorage.contains(blockHash)
      }
      .flatMap {
        case true =>
          persist(path)

        case false =>
          readInMemory {
            blockStorage.get(blockHash)
          }.flatMap {
            case Some(block) =>
              syncParent(sender, block, path)

            case None =>
              tryDownload(sender, blockHash)
                .flatMap {
                  case Some(block) =>
                    writeInMemory {
                      blockStorage.put(block)
                    } >> syncParent(sender, block, path)

                  case None =>
                    Timer[F].sleep(retryTimeout) >>
                      sync(sender, blockHash, path)
                }
          }
      }
  }

  private def syncParent(
      from: A#PKey,
      block: A#Block,
      path: List[A#Hash]
  ): F[Unit] = {
    val blockHash       = Block[A].blockHash(block)
    val parentBlockHash = Block[A].parentBlockHash(block)
    sync(from, parentBlockHash, blockHash :: path)
  }

  private def tryDownload(
      from: A#PKey,
      blockHash: A#Hash
  ): F[Option[A#Block]] =
    getBlock(from, blockHash).map(validateBlock(blockHash))

  private def validateBlock(requestedBlockHash: A#Hash)(
      maybeDownloadedBlock: Option[A#Block]
  ): Option[A#Block] =
    maybeDownloadedBlock.filter { block =>
      Block[A].blockHash(block) == requestedBlockHash
    }

  /** Move the blocks on the path from memory to persistent storage. */
  private def persist(
      path: List[A#Hash]
  ): F[Unit] =
    path match {
      case Nil =>
        ().pure[F]

      case blockHash :: rest =>
        readInMemory {
          blockStorage.get(blockHash)
        } flatMap {
          case None =>
            // Another download has already persisted it.
            persist(rest)

          case Some(block) =>
            storeRunner.runReadWrite {
              blockStorage.put(block)
            } >>
              writeInMemory {
                // There could be other, overlapping paths being downoaded,
                // but as long as they are on the call stack, it's okay to
                // create a forest here.
                blockStorage.deleteUnsafe(blockHash).void
              }
        }
    }

  private def readInMemory[A](query: KVStoreRead[N, A]): F[A] =
    inMemoryStoreRef.get.map(state.compile(query).run)

  private def writeInMemory[A](query: KVStore[N, A]): F[A] =
    inMemoryStoreRef.modify { store =>
      state.compile(query).run(store).value
    }
}

object BlockSynchronizer {

  /** Send a network request to get a block. */
  type GetBlock[F[_], A <: Agreement] = (A#PKey, A#Hash) => F[Option[A#Block]]

  /** Create a block synchronizer resource. Stop any background downloads when released. */
  def apply[F[_]: Sync: Timer, N, A <: Agreement: Block](
      blockStorage: BlockStorage[N, A],
      getBlock: GetBlock[F, A]
  )(implicit
      storeRunner: KVStoreRunner[F, N]
  ): Resource[F, BlockSynchronizer[F, N, A]] =
    Resource.liftF {
      for {
        inMemoryStoreRef <- Ref.of[F, KVStoreState[N]#Store](Map.empty)
        synchronizer = new BlockSynchronizer[F, N, A](
          blockStorage,
          getBlock,
          inMemoryStoreRef
        )
      } yield synchronizer
    }
}
