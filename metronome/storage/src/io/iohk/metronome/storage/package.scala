package io.iohk.metronome

import cats.free.Free
import cats.data.State

package object storage {
  type KVStore[N, A] = Free[({ type L[A] = KVStoreOp[N, A] })#L, A]
}
