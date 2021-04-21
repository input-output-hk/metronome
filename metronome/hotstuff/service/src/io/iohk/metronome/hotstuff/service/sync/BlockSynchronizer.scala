package io.iohk.metronome.hotstuff.service.sync

import cats.implicits._
import cats.effect.{Sync, Timer, Resource, Concurrent, ContextShift}
import cats.effect.concurrent.Semaphore
import io.iohk.metronome.core.fibers.FiberMap
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  QuorumCertificate,
  Block
}
import io.iohk.metronome.hotstuff.service.storage.BlockStorage
import io.iohk.metronome.storage.{InMemoryKVStore, KVStoreRunner}
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
    inMemoryStore: KVStoreRunner[F, N],
    fiberMap: FiberMap[F, A#PKey],
    semaphore: Semaphore[F],
    retryTimeout: FiniteDuration = 5.seconds
)(implicit storeRunner: KVStoreRunner[F, N]) {

  // We must take care not to insert blocks into storage and risk losing
  // the pointer to them in a restart, hence keeping the unfinished tree
  // in memory until we find a parent we do have in storage, then
  // insert them in the opposite order.

  /** Download all blocks up to the one included in the Quorum Certificate. */
  def sync(
      sender: A#PKey,
      quorumCertificate: QuorumCertificate[A]
  ): F[Unit] =
    // Only initiating one download from a given peer, so even if we try to sync
    // an overlapping path, we don't end up asking the same block multiple times
    // and re-inserting it into the memory store if another download removed it.
    fiberMap
      .submit(sender) {
        download(sender, quorumCertificate.blockHash, Nil)
      }
      .flatten
      .flatMap(persist(quorumCertificate.blockHash, _))

  /** Download a block and all of its ancestors into the in-memory block store.
    *
    * Returns the path from the greatest ancestor that had to be downloaded
    * to the originally requested block, so that we can persist them in that order.
    *
    * The path is maintained separately from the in-memory store in case another
    * ongoing download would re-insert something on a path already partially removed
    * resulting in a forest that cannot be traversed fully.
    */
  private def download(
      from: A#PKey,
      blockHash: A#Hash,
      path: List[A#Hash]
  ): F[List[A#Hash]] = {
    storeRunner
      .runReadOnly {
        blockStorage.contains(blockHash)
      }
      .flatMap {
        case true =>
          path.pure[F]

        case false =>
          inMemoryStore
            .runReadOnly {
              blockStorage.get(blockHash)
            }
            .flatMap {
              case Some(block) =>
                downloadParent(from, block, path)

              case None =>
                getAndValidateBlock(from, blockHash)
                  .flatMap {
                    case Some(block) =>
                      inMemoryStore.runReadWrite {
                        blockStorage.put(block)
                      } >> downloadParent(from, block, path)

                    case None =>
                      // TODO: Trace.
                      Timer[F].sleep(retryTimeout) >>
                        download(from, blockHash, path)
                  }
            }
      }
  }

  private def downloadParent(
      from: A#PKey,
      block: A#Block,
      path: List[A#Hash]
  ): F[List[A#Hash]] = {
    val blockHash       = Block[A].blockHash(block)
    val parentBlockHash = Block[A].parentBlockHash(block)
    download(from, parentBlockHash, blockHash :: path)
  }

  /** Try downloading the block from the source and perform basic content validation. */
  private def getAndValidateBlock(
      from: A#PKey,
      blockHash: A#Hash
  ): F[Option[A#Block]] =
    getBlock(from, blockHash).map { maybeBlock =>
      maybeBlock.filter { block =>
        Block[A].blockHash(block) == blockHash &&
        Block[A].isValid(block)
      }
    }

  /** See how far we can go in memory from the original block hash we asked for,
    * which indicates the blocks that no concurrent download has persisted yet,
    * then persist the rest.
    *
    * Only doing oine persist operation at a time to make sure there's no competition
    * in the insertion order of the path elements among concurrent downloads.
    */
  private def persist(
      blockHash: A#Hash,
      path: List[A#Hash]
  ): F[Unit] =
    semaphore.withPermit {
      inMemoryStore
        .runReadOnly {
          blockStorage.getPathFromRoot(blockHash).map(_.toSet)
        }
        .flatMap { unpersisted =>
          persist(path, unpersisted)
        }
    }

  /** Move the blocks on the path from memory to persistent storage. */
  private def persist(
      path: List[A#Hash],
      unpersisted: Set[A#Hash]
  ): F[Unit] =
    path match {
      case Nil =>
        ().pure[F]

      case blockHash :: rest =>
        inMemoryStore
          .runReadWrite {
            for {
              maybeBlock <- blockStorage.get(blockHash).lift
              // There could be other, overlapping paths being downloaded,
              // but as long as they are on the call stack, it's okay to
              // create a forest here.
              _ <- blockStorage.deleteUnsafe(blockHash)
            } yield maybeBlock
          }
          .flatMap {
            case Some(block) if unpersisted(blockHash) =>
              storeRunner
                .runReadWrite {
                  blockStorage.put(block)
                }
            case _ =>
              // Another download has already persisted it.
              ().pure[F]

          } >>
          persist(rest, unpersisted)
    }
}

object BlockSynchronizer {

  /** Send a network request to get a block. */
  type GetBlock[F[_], A <: Agreement] = (A#PKey, A#Hash) => F[Option[A#Block]]

  /** Create a block synchronizer resource. Stop any background downloads when released. */
  def apply[F[_]: Concurrent: ContextShift: Timer, N, A <: Agreement: Block](
      blockStorage: BlockStorage[N, A],
      getBlock: GetBlock[F, A]
  )(implicit
      storeRunner: KVStoreRunner[F, N]
  ): Resource[F, BlockSynchronizer[F, N, A]] =
    for {
      fiberMap      <- FiberMap[F, A#PKey]()
      semaphore     <- Resource.liftF(Semaphore[F](1))
      inMemoryStore <- Resource.liftF(InMemoryKVStore[F, N])
      synchronizer = new BlockSynchronizer[F, N, A](
        blockStorage,
        getBlock,
        inMemoryStore,
        fiberMap,
        semaphore
      )
    } yield synchronizer
}
