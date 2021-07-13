package io.iohk.metronome.hotstuff.service.sync

import cats.implicits._
import cats.data.NonEmptyVector
import cats.effect.{Sync, Timer, Concurrent, ContextShift}
import cats.effect.concurrent.Semaphore
import io.iohk.metronome.hotstuff.consensus.Federation
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  QuorumCertificate,
  Block
}
import io.iohk.metronome.hotstuff.service.storage.BlockStorage
import io.iohk.metronome.storage.{InMemoryKVStore, KVStoreRunner}
import scala.concurrent.duration._
import scala.util.Random
import scala.util.control.NoStackTrace

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
    publicKey: A#PKey,
    federation: Federation[A#PKey],
    blockStorage: BlockStorage[N, A],
    getBlock: BlockSynchronizer.GetBlock[F, A],
    inMemoryStore: KVStoreRunner[F, N],
    semaphore: Semaphore[F],
    retryTimeout: FiniteDuration = 5.seconds
)(implicit storeRunner: KVStoreRunner[F, N]) {
  import BlockSynchronizer.DownloadFailedException

  private val otherPublicKeys =
    federation.publicKeys.filterNot(_ == publicKey)

  // We must take care not to insert blocks into storage and risk losing
  // the pointer to them in a restart, hence keeping the unfinished tree
  // in memory until we find a parent we do have in storage, then
  // insert them in the opposite order.

  /** Download all blocks up to the one included in the Quorum Certificate.
    *
    * Only expected to be called once per sender at the same time, otherwise
    * it may request the same ancestor block multiple times concurrently.
    *
    * This could be managed with internal queueing, but not having that should
    * make it easier to cancel all calling fibers and discard the synchronizer
    * instance and its in-memory store, do state syncing, then replace it with
    * a fresh one.
    */
  def sync(
      sender: A#PKey,
      quorumCertificate: QuorumCertificate[A]
  ): F[Unit] =
    for {
      path <- download(sender, quorumCertificate.blockHash, Nil)
      _    <- persist(quorumCertificate.blockHash, path)
    } yield ()

  /** Download the block in the Quorum Certificate without ancestors.
    *
    * Return it without being persisted.
    *
    * Unlike `sync`, which is expected to be canceled if consensus times out,
    * or be satisfied by alternative downloads happening concurrently, this
    * method returns and error if it cannot download the block after a certain
    * number of attempts, from any of the sources. This is becuause its primary
    * use is during state syncing where this is the only operation, and if for
    * any reason the block would be gone from everyone honest members' storage,
    * we have to try something else.
    */
  def getBlockFromQuorumCertificate(
      sources: NonEmptyVector[A#PKey],
      quorumCertificate: QuorumCertificate[A]
  ): F[Either[DownloadFailedException[A], A#Block]] = {
    val otherSources = sources.filterNot(_ == publicKey).toList

    def loop(
        sources: List[A#PKey]
    ): F[Either[DownloadFailedException[A], A#Block]] = {
      sources match {
        case Nil =>
          new DownloadFailedException(
            quorumCertificate.blockHash,
            sources.toVector
          ).asLeft[A#Block].pure[F]

        case source :: alternatives =>
          getAndValidateBlock(source, quorumCertificate.blockHash, otherSources)
            .flatMap {
              case None =>
                loop(alternatives)
              case Some(block) =>
                block.asRight[DownloadFailedException[A]].pure[F]
            }
      }
    }

    storeRunner
      .runReadOnly {
        blockStorage.get(quorumCertificate.blockHash)
      }
      .flatMap {
        case None        => loop(Random.shuffle(otherSources))
        case Some(block) => block.asRight[DownloadFailedException[A]].pure[F]
      }
  }

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

  /** Try downloading the block from the source and perform basic content validation.
    *
    * If the download fails, try random alternative sources in the federation.
    */
  private def getAndValidateBlock(
      from: A#PKey,
      blockHash: A#Hash,
      alternativeSources: Seq[A#PKey] = otherPublicKeys
  ): F[Option[A#Block]] = {
    def fetch(from: A#PKey) =
      getBlock(from, blockHash)
        .map { maybeBlock =>
          maybeBlock.filter { block =>
            Block[A].blockHash(block) == blockHash &&
            Block[A].isValid(block)
          }
        }

    def loop(sources: List[A#PKey]): F[Option[A#Block]] =
      sources match {
        case Nil => none.pure[F]
        case from :: sources =>
          fetch(from).flatMap {
            case None  => loop(sources)
            case block => block.pure[F]
          }
      }

    loop(List(from)).flatMap {
      case None =>
        loop(Random.shuffle(alternativeSources.filterNot(_ == from).toList))
      case block =>
        block.pure[F]
    }
  }

  /** See how far we can go in memory from the original block hash we asked for,
    * which indicates the blocks that no concurrent download has persisted yet,
    * then persist the rest.
    *
    * Only doing one persist operation at a time to make sure there's no competition
    * in the insertion order of the path elements among concurrent downloads.
    */
  private def persist(
      targetBlockHash: A#Hash,
      path: List[A#Hash]
  ): F[Unit] =
    semaphore.withPermit {
      inMemoryStore
        .runReadOnly {
          blockStorage.getPathFromRoot(targetBlockHash)
        }
        .flatMap { unpersisted =>
          persistAndClear(path, unpersisted.toSet)
        }
    }

  /** Move the blocks on the path from memory to persistent storage.
    *
    * `path` and `unpersisted` can be different when a concurrent download
    * re-inserts some ancestor block into the in-memory store that another
    * download has already removed during persistence. The `unpersisted`
    * set only contains block that need to be inserted into persistent
    * storage, but all `path` elements have to be visited to make sure
    * nothing is left in the in-memory store, leaking memory.
    */
  private def persistAndClear(
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
          persistAndClear(rest, unpersisted)
    }
}

object BlockSynchronizer {

  class DownloadFailedException[A <: Agreement](
      blockHash: A#Hash,
      sources: Seq[A#PKey]
  ) extends RuntimeException(
        s"Failed to download block ${blockHash} from ${sources.size} sources."
      )
      with NoStackTrace

  /** Send a network request to get a block. */
  type GetBlock[F[_], A <: Agreement] = (A#PKey, A#Hash) => F[Option[A#Block]]

  /** Create a block synchronizer resource. Stop any background downloads when released. */
  def apply[F[_]: Concurrent: ContextShift: Timer, N, A <: Agreement: Block](
      publicKey: A#PKey,
      federation: Federation[A#PKey],
      blockStorage: BlockStorage[N, A],
      getBlock: GetBlock[F, A]
  )(implicit
      storeRunner: KVStoreRunner[F, N]
  ): F[BlockSynchronizer[F, N, A]] =
    for {
      semaphore     <- Semaphore[F](1)
      inMemoryStore <- InMemoryKVStore[F, N]
      synchronizer = new BlockSynchronizer[F, N, A](
        publicKey,
        federation,
        blockStorage,
        getBlock,
        inMemoryStore,
        semaphore
      )
    } yield synchronizer
}
