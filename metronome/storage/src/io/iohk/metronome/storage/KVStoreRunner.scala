package io.iohk.metronome.storage

/** Convenience interface to turn KVStore queries into effects. */
trait KVStoreRunner[F[_], N] {
  def runReadOnly[A](query: KVStoreRead[N, A]): F[A]
  def runReadWrite[A](query: KVStore[N, A]): F[A]
}
