package io.iohk.metronome

import cats.free.Free

package object storage {

  /** Read/Write operations over a key-value store. */
  type KVStore[N, A] = Free[({ type L[A] = KVStoreOp[N, A] })#L, A]

  /** Read-only operations over a key-value store. */
  type KVStoreRead[N, A] = Free[({ type L[A] = KVStoreReadOp[N, A] })#L, A]

  /** Extension method to lift a read-only operation to read-write. */
  implicit class KVStoreReadOps[N, A](val read: KVStoreRead[N, A])
      extends AnyVal {
    def lift: KVStore[N, A] = KVStore.instance[N].lift(read)
  }
}
