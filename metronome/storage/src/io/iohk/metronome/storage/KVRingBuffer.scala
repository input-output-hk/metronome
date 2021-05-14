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

  private implicit val metaKeyEncoder: Encoder[MetaKey[_]] = {
    import scodec.codecs._
    import scodec.codecs.implicits._

    val bucketIndexCodec                        = provide(BucketIndex)
    val bucketCodec: Codec[Bucket[_]]           = Codec.deriveLabelledGeneric
    val keyRefCountCodec: Codec[KeyRefCount[K]] = Codec.deriveLabelledGeneric

    discriminated[MetaKey[_]]
      .by(uint2)
      .typecase(0, bucketIndexCodec)
      .typecase(1, bucketCodec)
      .typecase(2, keyRefCountCodec)
      .asEncoder
  }

  private def getMetaData[V: Decoder](key: MetaKey[V]) =
    KVStore[N].get[MetaKey[V], V](metaNamespace, key)

  private def putMetaData[V: Encoder](key: MetaKey[V], value: V) =
    KVStore[N].put(metaNamespace, key, value)

  private def setRefCount(key: K, count: Int) =
    if (count > 0)
      putMetaData[Int](KeyRefCount(key), count)
    else
      KVStore[N].delete(metaNamespace, KeyRefCount(key))

  private def getRefCount(key: K) =
    getMetaData[Int](KeyRefCount(key)).map(_ getOrElse 0)

  /** Return the index of the next bucket to write the data into. */
  private def nextIndex(maybeIndex: Option[Int]): Int =
    maybeIndex.fold(0)(index => (index + 1) % maxHistorySize)

  private def add(key: K, value: V) =
    getRefCount(key).flatMap { cnt =>
      if (cnt == 0)
        setRefCount(key, 1) >> coll.put(key, value)
      else
        setRefCount(key, cnt + 1)
    }

  private def maybeRemove(key: K) =
    getRefCount(key).flatMap { cnt =>
      if (cnt > 1)
        setRefCount(key, cnt - 1).as(none[K])
      else
        setRefCount(key, 0) >> coll.delete(key).as(key.some)
    }

  /** Save a new item and remove the oldest one, if we reached
    * the maximum history size.
    *
    * Returns the key which has been evicted, unless it's still
    * referenced by something or the history hasn't reached maximum
    * size yet.
    */
  def put(key: K, value: V): KVStore[N, Option[K]] = {
    for {
      index          <- getMetaData(BucketIndex).map(nextIndex)
      maybeOldestKey <- getMetaData(Bucket[K](index))
      maybeRemoved <- maybeOldestKey match {
        case Some(oldestKey) if oldestKey == key =>
          KVStore[N].pure(none[K])

        case Some(oldestKey) =>
          add(key, value) >> maybeRemove(oldestKey)

        case None =>
          add(key, value).as(none[K])
      }
      _ <- putMetaData(Bucket(index), key)
      _ <- putMetaData(BucketIndex, index)
    } yield maybeRemoved
  }

  /** Retrieve an item by hash, if we still have it. */
  def get(key: K): KVStoreRead[N, Option[V]] =
    coll.read(key)
}

object KVRingBuffer {

  /** Keys for different pieces of meta-data stored under a single namespace. */
  sealed trait MetaKey[+V]

  /** Key under which the last written index of the ring buffer is stored. */
  case object BucketIndex extends MetaKey[Int]

  /** Contents of a ring buffer bucket by index. */
  case class Bucket[V](index: Int) extends MetaKey[V] {
    assert(index >= 0)
  }

  /** Number of buckets currently pointing at a key. */
  case class KeyRefCount[K](key: K) extends MetaKey[Int]
}
