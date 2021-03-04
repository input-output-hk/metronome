package io.iohk.metronome.storage

import cats.{~>}
import cats.data.State
import io.iohk.metronome.storage.KVStoreOp.{Put, Get, Delete}

/** A pure implementation of the Free interpreter using the State monad.
  *
  * It uses a specific namespace type, which is common to all collections.
  */
class KVStoreState[N] {

  // Ignoring the Codec for the in-memory use case.
  type Store                = Map[N, Map[Any, Any]]
  type KVNamespacedState[A] = State[Store, A]
  type KVNamespacedOp[A]    = ({ type L[A] = KVStoreOp[N, A] })#L[A]

  private val stateCompiler: KVNamespacedOp ~> KVNamespacedState =
    new (KVNamespacedOp ~> KVNamespacedState) {
      def apply[A](fa: KVNamespacedOp[A]): KVNamespacedState[A] =
        fa match {
          case Put(n, k, v) =>
            State.modify { nkvs =>
              val kvs = nkvs.getOrElse(n, Map.empty)
              nkvs.updated(n, kvs.updated(k, v))
            }

          case Get(n, k) =>
            State.inspect { nkvs =>
              for {
                kvs <- nkvs.get(n)
                v   <- kvs.get(k)
                // NOTE: This should be fine as long as we access it through
                // `KVCollection` which works with 1 kind of value;
                // otherwise we could change the effect to allow errors:
                // `State[Store, Either[Throwable, A]]`

                // The following cast would work but it's not required:
                // .asInstanceOf[A]
              } yield v
            }

          case Delete(n, k) =>
            State.modify { nkvs =>
              val kvs = nkvs.getOrElse(n, Map.empty) - k
              if (kvs.isEmpty) nkvs - n else nkvs.updated(n, kvs)
            }
        }
    }

  /** Compile a KVStore program to a State monad, which can be executed like:
    *
    * `new KvStoreState[String].compile(program).run(Map.empty).value`
    */
  def compile[A](program: KVStore[N, A]): KVNamespacedState[A] =
    program.foldMap(stateCompiler)
}