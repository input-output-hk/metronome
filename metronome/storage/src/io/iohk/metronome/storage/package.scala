package io.iohk.metronome

import cats.free.Free

package object storage {

  /** Read/Write operations over a key-value store. */
  type KVStore[N, A] = Free[({ type L[A] = KVStoreOp[N, A] })#L, A]

  /** Read-only operations over a key-value store. */
  type KVStoreRead[N, A] = Free[({ type L[A] = KVStoreReadOp[N, A] })#L, A]
}
