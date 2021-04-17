package io.iohk.metronome.storage

import cats.{~>}
import cats.free.Free
import cats.free.Free.liftF
import scodec.Codec

/** Helper methods to read/write a key-value store. */
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

    type KVNamespacedOp[A]     = ({ type L[A] = KVStoreOp[N, A] })#L[A]
    type KVNamespacedReadOp[A] = ({ type L[A] = KVStoreReadOp[N, A] })#L[A]

    def unit: KVStore[N, Unit] = KVStore.unit[N]

    def pure[A](a: A) = KVStore.pure[N, A](a)

    /** Insert or replace a value under a key. */
    def put[K: Codec, V: Codec](
        namespace: N,
        key: K,
        value: V
    ): KVStore[N, Unit] =
      liftF[KVNamespacedOp, Unit](
        Put[N, K, V](namespace, key, value)
      )

    /** Get a value under a key, if it exists. */
    def get[K: Codec, V: Codec](namespace: N, key: K): KVStore[N, Option[V]] =
      liftF[KVNamespacedOp, Option[V]](
        Get[N, K, V](namespace, key)
      )

    /** Delete a value under a key. */
    def delete[K: Codec](namespace: N, key: K): KVStore[N, Unit] =
      liftF[KVNamespacedOp, Unit](
        Delete[N, K](namespace, key)
      )

    /** Apply a function on a value, if it exists. */
    def update[K: Codec, V: Codec](namespace: N, key: K)(
        f: V => V
    ): KVStore[N, Unit] =
      get[K, V](namespace, key).flatMap {
        case None        => unit
        case Some(value) => put(namespace, key, f(value))
      }

    /** Insert, update or delete a value, depending on whether it exists. */
    def alter[K: Codec, V: Codec](namespace: N, key: K)(
        f: Option[V] => Option[V]
    ): KVStore[N, Unit] =
      get[K, V](namespace, key).flatMap { current =>
        (current, f(current)) match {
          case (None, None)     => unit
          case (_, Some(value)) => put(namespace, key, value)
          case (Some(_), None)  => delete(namespace, key)
        }
      }

    /** Lift a read-only operation into a read-write one, so that we can chain them together. */
    def lift[A](read: KVStoreRead[N, A]): KVStore[N, A] =
      read.mapK(liftCompiler)

    private val liftCompiler: KVNamespacedReadOp ~> KVNamespacedOp =
      new (KVNamespacedReadOp ~> KVNamespacedOp) {
        def apply[A](fa: KVNamespacedReadOp[A]): KVNamespacedOp[A] =
          fa
      }
  }
}
