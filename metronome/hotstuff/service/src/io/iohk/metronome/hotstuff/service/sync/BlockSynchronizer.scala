package io.iohk.metronome.hotstuff.service.sync

import cats.implicits._
import cats.effect.{Sync, Timer, Resource, Concurrent, ContextShift}
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
    download(sender, quorumCertificate.blockHash) >>
      persist(quorumCertificate.blockHash)

  /** Download a block and all of its ancestors into the in-memory block store. */
  private def download(
      from: A#PKey,
      blockHash: A#Hash
  ): F[Unit] = {
    storeRunner
      .runReadOnly {
        blockStorage.contains(blockHash)
      }
      .flatMap {
        case true =>
          ().pure[F]

        case false =>
          inMemoryStore
            .runReadOnly {
              blockStorage.get(blockHash)
            }
            .flatMap {
              case Some(block) =>
                downloadParent(from, block)

              case None =>
                getAndValidateBlock(from, blockHash)
                  .flatMap {
                    case Some(block) =>
                      inMemoryStore.runReadWrite {
                        blockStorage.put(block)
                      } >> downloadParent(from, block)

                    case None =>
                      Timer[F].sleep(retryTimeout) >>
                        download(from, blockHash)
                  }
            }
      }
  }

  private def downloadParent(
      from: A#PKey,
      block: A#Block
  ): F[Unit] =
    download(from, Block[A].parentBlockHash(block))

  /** Try downloading the block from the source and perform basic content validation.
    *
    * Only send one download request to a peer at any given time.
    */
  private def getAndValidateBlock(
      from: A#PKey,
      blockHash: A#Hash
  ): F[Option[A#Block]] =
    fiberMap
      .submit(from) {
        getBlock(from, blockHash).map { maybeBlock =>
          maybeBlock.filter { block =>
            Block[A].blockHash(block) == blockHash &&
            Block[A].isValid(block)
          }
        }
      }
      .flatten

  /** Persist the path that leads from the its greatest ancestor in the
    * in-memory tree to the given block hash.
    */
  private def persist(blockHash: A#Hash): F[Unit] =
    inMemoryStore.runReadOnly {
      blockStorage.getPathFromRoot(blockHash)
    } flatMap { path =>
      persist(path)
    }

  /** Move the blocks on the path from memory to persistent storage. */
  private def persist(
      path: List[A#Hash]
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
            case None =>
              // Another download has already persisted it.
              ().pure[F]

            case Some(block) =>
              storeRunner.runReadWrite {
                blockStorage.put(block)
              }
          } >>
          persist(rest)
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
      inMemoryStore <- Resource.liftF(InMemoryKVStore[F, N])
      synchronizer = new BlockSynchronizer[F, N, A](
        blockStorage,
        getBlock,
        inMemoryStore,
        fiberMap
      )
    } yield synchronizer
}
