package io.iohk.metronome.storage

import cats.implicits._
import cats.effect.Sync
import cats.effect.concurrent.Ref

/** Simple in-memory key-value store based on `KVStoreState` and `KVStoreRunner`. */
object InMemoryKVStore {
  def apply[F[_]: Sync, N]: F[KVStoreRunner[F, N]] =
    Ref.of[F, KVStoreState[N]#Store](Map.empty).map(apply(_))

  def apply[F[_]: Sync, N](
      storeRef: Ref[F, KVStoreState[N]#Store]
  ): KVStoreRunner[F, N] =
    new KVStoreState[N] with KVStoreRunner[F, N] {
      def runReadOnly[A](query: KVStoreRead[N, A]): F[A] =
        storeRef.get.map(compile(query).run)

      def runReadWrite[A](query: KVStore[N, A]): F[A] =
        storeRef.modify { store =>
          compile(query).run(store).value
        }
    }
}
