package io.iohk.metronome.checkpointing.service.storage

import cats.implicits._
import io.iohk.metronome.checkpointing.models.Ledger
import io.iohk.metronome.storage.{KVCollection, KVStore, KVStoreRead}
import scodec.{Decoder, Encoder, Codec}

/** Storing the committed and executed checkpoint ledger.
  *
  * Strictly speaking the application only needs the committed state,
  * since it has been signed by the federation and we know it's not
  * going to be rolled back. Uncommitted state can be kept in memory.
  *
  * However we want to support other nodes catching up by:
  * 1. requesting the latest Commit Q.C., then
  * 2. requesting the block the Commit Q.C. points at, then
  * 3. requesting the ledger state the header points at.
  *
  * We have to allow some time before we get rid of historical state,
  * so that it doesn't disappear between step 2 and 3, resulting in
  * nodes trying and trying to catch up but always missing the beat.
  *
  * Therefore we keep a collection of the last N ledgers in a ring buffer.
  */
class LedgerStorage[N](
    ledgerColl: KVCollection[N, Ledger.Hash, Ledger],
    ledgerMetaNamespace: N,
    maxHistorySize: Int
)(implicit codecH: Codec[Ledger.Hash]) {
  import LedgerStorage._
  import scodec.codecs.implicits.implicitIntCodec

  private implicit val kvn  = KVStore.instance[N]
  private implicit val kvrn = KVStoreRead.instance[N]

  private def getMetaData[V: Decoder](key: MetaKey[V]) =
    KVStore[N].get[MetaKey[V], V](ledgerMetaNamespace, key)

  private def putMetaData[V: Encoder](key: MetaKey[V], value: V) =
    KVStore[N].put(ledgerMetaNamespace, key, value)

  private def nextIndex(maybeIndex: Option[Int]): Int =
    maybeIndex.fold(0)(index => (index + 1) % maxHistorySize)

  /** Save a new ledger and remove the oldest one, if we reached
    * the maximum history size. Since we only store committed
    * state, they form a chain. They will always be retrieved
    * by going through a block pointing at them directly.
    */
  def put(ledger: Ledger): KVStore[N, Unit] =
    for {
      index <- getMetaData(BucketIndex).map(nextIndex)
      ledgerHash = ledger.hash
      maybeOldestHash <- getMetaData(Bucket(index))
      _ <- maybeOldestHash match {
        case Some(oldestHash) if oldestHash == ledgerHash =>
          KVStore[N].unit

        case Some(oldestHash) =>
          ledgerColl.put(ledgerHash, ledger) >>
            ledgerColl.delete(oldestHash)

        case None =>
          ledgerColl.put(ledgerHash, ledger)
      }
      _ <- putMetaData(Bucket(index), ledgerHash)
      _ <- putMetaData(BucketIndex, index)
    } yield ()

  /** Retrieve a ledger by state hash, if we still have it. */
  def get(ledgerHash: Ledger.Hash): KVStoreRead[N, Option[Ledger]] =
    ledgerColl.read(ledgerHash)
}

object LedgerStorage {

  /** Keys for different pieces of meta-data stored under a single namespace. */
  sealed trait MetaKey[V]

  /** Key under which the current bucket index of the ring buffer is stored. */
  case object BucketIndex extends MetaKey[Int]

  /** Contents of a bucket by index. */
  case class Bucket(index: Int) extends MetaKey[Ledger.Hash]

  implicit val metaKeyEncoder: Encoder[MetaKey[_]] =
    scodec.codecs.implicits.implicitIntCodec.asEncoder.contramap {
      case BucketIndex   => -1
      case Bucket(index) => index
    }
}
