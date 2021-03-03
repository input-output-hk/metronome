package io.iohk.metronome.storage

import cats.free.Free
import cats.free.Free.liftF
import scodec.Codec

/** Storage for a specific type of data, e.g. blocks, in a given namespace.
  *
  * We should be able to string together KVStore operations across multiple
  * collections and execute them in one batch.
  */
class KVCollection[N, K: Codec, V: Codec](namespace: N) {

  import KVStoreOp._

  type KVNamespacedOp[A] = ({ type L[A] = KVStoreOp[N, A] })#L[A]

  /** Put a value under a key. */
  def put(key: K, value: V): KVStore[N, Unit] =
    liftF[KVNamespacedOp, Unit](
      Put[N, K, V](namespace, key, value)
    )

  /** Get a value by key, if it exists. */
  def get(key: K): KVStore[N, Option[V]] =
    liftF[KVNamespacedOp, Option[V]](
      Get[N, K, V](namespace, key)
    )

  /** Delete a value by key. */
  def delete(key: K): KVStore[N, Unit] =
    liftF[KVNamespacedOp, Unit](
      Delete[N, K](namespace, key)
    )

  /** Update a key by getting the value and applying a function on it, if the value exists. */
  def update(key: K, f: V => V): KVStore[N, Unit] =
    get(key).flatMap {
      case None    => Free.pure[KVNamespacedOp, Unit](())
      case Some(v) => put(key, f(v))
    }
}
