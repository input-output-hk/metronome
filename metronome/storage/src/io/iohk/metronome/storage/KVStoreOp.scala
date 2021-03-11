package io.iohk.metronome.storage

import scodec.Codec

/** Representing key-value storage operations as a Free Monad,
  * so that we can pick an execution strategy that best fits
  * the database technology at hand:
  * - execute multiple writes atomically by batching
  * - execute all reads and writes in a transaction
  *
  * The key-value store is expected to store binary data,
  * so a scodec.Codec is required for all operations to
  * serialize the keys and the values.
  *
  * https://typelevel.org/cats/datatypes/freemonad.html
  */
sealed trait KVStoreOp[N, A]
sealed trait KVStoreReadOp[N, A]  extends KVStoreOp[N, A]
sealed trait KVStoreWriteOp[N, A] extends KVStoreOp[N, A]

object KVStoreOp {
  case class Put[N, K, V](namespace: N, key: K, value: V)(implicit
      val keyCodec: Codec[K],
      val valueCodec: Codec[V]
  ) extends KVStoreWriteOp[N, Unit]

  case class Get[N, K, V](namespace: N, key: K)(implicit
      val keyCodec: Codec[K],
      val valueCodec: Codec[V]
  ) extends KVStoreReadOp[N, Option[V]]

  case class Delete[N, K](namespace: N, key: K)(implicit val keyCodec: Codec[K])
      extends KVStoreWriteOp[N, Unit]
}
