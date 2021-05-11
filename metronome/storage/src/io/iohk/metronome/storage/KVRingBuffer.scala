package io.iohk.metronome.storage

import cats.implicits._
import scodec.{Decoder, Encoder, Codec}

/** Storing the last N items inserted into a collection. */
class KVRingBuffer[N, K, V](
    coll: KVCollection[N, K, V],
    metaNamespace: N,
    maxHistorySize: Int
)(implicit codecK: Codec[K]) {
  require(maxHistorySize > 0, "Has to store at least one item in the buffer.")

  import KVRingBuffer._
  import scodec.codecs.implicits.implicitIntCodec

  private implicit val kvn = KVStore.instance[N]

  private def getMetaData[V: Decoder](key: MetaKey[V]) =
    KVStore[N].get[MetaKey[V], V](metaNamespace, key)

  private def putMetaData[V: Encoder](key: MetaKey[V], value: V) =
    KVStore[N].put(metaNamespace, key, value)

  /** Return the index of the next bucket to write the data into. */
  private def nextIndex(maybeIndex: Option[Int]): Int =
    maybeIndex.fold(0)(index => (index + 1) % maxHistorySize)

  /** Save a new item and remove the oldest one, if we reached
    * the maximum history size.
    */
  def put(key: K, value: V): KVStore[N, Unit] =
    for {
      index          <- getMetaData(BucketIndex).map(nextIndex)
      maybeOldestKey <- getMetaData(Bucket[K](index))
      _ <- maybeOldestKey match {
        case Some(oldestKey) if oldestKey == key =>
          KVStore[N].unit

        case Some(oldestKey) =>
          coll.put(key, value) >>
            coll.delete(oldestKey)

        case None =>
          coll.put(key, value)
      }
      _ <- putMetaData(Bucket(index), key)
      _ <- putMetaData(BucketIndex, index)
    } yield ()

  /** Retrieve an item hash hash, if we still have it. */
  def get(key: K): KVStoreRead[N, Option[V]] =
    coll.read(key)
}

object KVRingBuffer {

  /** Keys for different pieces of meta-data stored under a single namespace. */
  sealed trait MetaKey[V]

  /** Key under which the last written index of the ring buffer is stored. */
  case object BucketIndex extends MetaKey[Int]

  /** Contents of a ring buffer bucket by index. */
  case class Bucket[V](index: Int) extends MetaKey[V] {
    assert(index >= 0)
  }

  implicit val metaKeyEncoder: Encoder[MetaKey[_]] =
    scodec.codecs.implicits.implicitIntCodec.asEncoder.contramap {
      case BucketIndex   => -1
      case Bucket(index) => index
    }
}
