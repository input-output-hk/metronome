package io.iohk.metronome.storage

import scodec.Codec

/** Storage for a specific type of data, e.g. blocks, in a given namespace.
  *
  * We should be able to string together KVStore operations across multiple
  * collections and execute them in one batch.
  */
class KVCollection[N, K: Codec, V: Codec](namespace: N) {

  private implicit val kvsRW = KVStore.instance[N]
  private implicit val kvsRO = KVStoreRead.instance[N]

  class ReadOnly {

    /** Get a value by key, if it exists. */
    def get(key: K): KVStoreRead[N, Option[V]] =
      KVStoreRead[N].get(namespace, key)
  }

  /** Project a read-only interface. */
  val readonly = new ReadOnly

  /** Put a value under a key. */
  def put(key: K, value: V): KVStore[N, Unit] =
    KVStore[N].put(namespace, key, value)

  /** Get a value by key, if it exists. */
  def get(key: K): KVStore[N, Option[V]] =
    KVStore[N].get(namespace, key)

  /** Delete a value by key. */
  def delete(key: K): KVStore[N, Unit] =
    KVStore[N].delete(namespace, key)

  /** Update a key by getting the value and applying a function on it, if the value exists. */
  def update(key: K, f: V => V): KVStore[N, Unit] =
    KVStore[N].update(namespace, key, f)
}
