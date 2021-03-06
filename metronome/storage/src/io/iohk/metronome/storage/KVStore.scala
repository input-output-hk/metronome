package io.iohk.metronome.storage

import cats.free.Free
import cats.free.Free.liftF
import scodec.Codec

object KVStore {

  def unit[N]: KVStore[N, Unit] =
    pure(())

  def pure[N, A](a: A): KVStore[N, A] =
    Free.pure(a)

  def instance[N]: Ops[N] = new Ops[N] {}

  def apply[N: Ops] = implicitly[Ops[N]]

  /** Scope all operations under the `N` type, which can be more convenient,
    * e.g. `KVStore[String].pure(1)` instead of `KVStore.pure[String, Int](1)`
    */
  trait Ops[N] {
    import KVStoreOp._

    type KVNamespacedOp[A] = ({ type L[A] = KVStoreOp[N, A] })#L[A]

    def unit: KVStore[N, Unit] = KVStore.unit[N]

    def pure[A](a: A) = KVStore.pure[N, A](a)

    def put[K: Codec, V: Codec](
        namespace: N,
        key: K,
        value: V
    ): KVStore[N, Unit] =
      liftF[KVNamespacedOp, Unit](
        Put[N, K, V](namespace, key, value)
      )

    def get[K: Codec, V: Codec](namespace: N, key: K): KVStore[N, Option[V]] =
      liftF[KVNamespacedOp, Option[V]](
        Get[N, K, V](namespace, key)
      )

    def delete[K: Codec](namespace: N, key: K): KVStore[N, Unit] =
      liftF[KVNamespacedOp, Unit](
        Delete[N, K](namespace, key)
      )

    def update[K: Codec, V: Codec](
        namespace: N,
        key: K,
        f: V => V
    ): KVStore[N, Unit] =
      get[K, V](namespace, key).flatMap {
        case None        => unit
        case Some(value) => put(namespace, key, f(value))
      }
  }
}
