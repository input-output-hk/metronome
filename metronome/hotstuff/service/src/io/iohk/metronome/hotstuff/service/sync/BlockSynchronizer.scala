package io.iohk.metronome.hotstuff.service.sync

import cats.implicits._
import cats.effect.{Resource, Sync}
import cats.effect.concurrent.Ref
import io.iohk.metronome.hotstuff.consensus.basic.{Agreement, QuorumCertificate}
import io.iohk.metronome.hotstuff.service.storage.BlockStorage
import io.iohk.metronome.storage.{KVStoreRunner, KVStoreState}

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
class BlockSynchronizer[F[_]: Sync, N, A <: Agreement](
    blockStorage: BlockStorage[N, A],
    getBlock: BlockSynchronizer.GetBlock[F, A],
    inMemoryStoreRef: Ref[F, KVStoreState[N]#Store]
)(implicit storeRunner: KVStoreRunner[F, N]) {

  // In memory KVStore query compiler.
  val state = new KVStoreState[N]

  /** Download all blocks up to the one included in the Quorum Certificate. */
  def sync(
      sender: A#PKey,
      quorumCertificate: QuorumCertificate[A]
  ): F[Unit] = {
    // TODO (PM-3134): Block sync.

    // We must take care not to insert blocks into storage and risk losing
    // the pointer to them in a restart. Maybe keep the unfinished tree
    // in memory until we find a parent we do have in storage, then
    // insert them in the opposite order, validating against the application side
    // as we go along, finally responding to the requestor.
    storeRunner
      .runReadOnly {
        blockStorage.contains(quorumCertificate.blockHash)
      }
      .flatMap {
        case true  => ().pure[F]
        case false => ??? // TODO: Manage downloads.
      }
  }

}

object BlockSynchronizer {

  /** Send a network request to get a block. */
  type GetBlock[F[_], A <: Agreement] = (A#PKey, A#Hash) => F[Option[A#Block]]

  /** Create a block synchronizer resource. Stop any background downloads when released. */
  def apply[F[_]: Sync, N, A <: Agreement](
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
