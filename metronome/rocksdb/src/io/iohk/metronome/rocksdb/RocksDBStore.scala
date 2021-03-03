package io.iohk.metronome.rocksdb

import cats._
import cats.implicits._
import cats.data.StateT
import cats.effect.{Resource, Sync}
import io.iohk.metronome.storage.{KVStore, KVStoreOp}
import io.iohk.metronome.storage.KVStoreOp.{Put, Get, Delete}
import org.rocksdb._
import scodec.Codec
import scodec.bits.BitVector

class RocksDBStore[F[_]: Sync](
    db: RocksDB,
    handles: Map[RocksDBStore.Namespace, ColumnFamilyHandle],
    readOptions: ReadOptions
) {
  import RocksDBStore.{Namespace, DBQuery}

  type BatchState        = (WriteOptions, WriteBatch)
  type DBQueryState[A]   = ({ type L[A] = StateT[F, BatchState, A] })#L[A]
  type KVNamespacedOp[A] = ({ type L[A] = KVStoreOp[Namespace, A] })#L[A]

  /** Collect writes in a batch, until we either get to the end, or there's a read.
    * This way writes are atomic, and reads can see their effect.
    *
    * In the next version of RocksDB we can use transactions to make reads and writes
    * run in isolation.
    */
  private val batchingCompiler: KVNamespacedOp ~> DBQueryState =
    new (KVNamespacedOp ~> DBQueryState) {
      def apply[A](fa: KVNamespacedOp[A]): DBQueryState[A] =
        fa match {
          case op @ Put(n, k, v) =>
            StateT.modifyF { case (opts, batch) =>
              for {
                kbs <- encode(k)(op.keyCodec)
                vbs <- encode(v)(op.valueCodec)
                _ = batch.put(handles(n), kbs, vbs)
              } yield (opts, batch)
            }

          case op @ Get(n, k) =>
            StateT.modifyF(executeBatch).flatMap { _ =>
              StateT.liftF {
                for {
                  kbs  <- encode(k)(op.keyCodec)
                  mvbs <- Sync[F].delay(Option(db.get(handles(n), kbs)))
                  mv <- mvbs match {
                    case None =>
                      none.pure[F]

                    case Some(bytes) =>
                      decode(bytes)(op.valueCodec).map(_.some)
                  }
                } yield mv
              }
            }

          case op @ Delete(n, k) =>
            StateT.modifyF { case (opts, batch) =>
              for {
                kbs <- encode(k)(op.keyCodec)
                _ = batch.delete(handles(n), kbs)
              } yield (opts, batch)
            }
        }
    }

  private def executeBatch(state: BatchState): F[BatchState] = state match {
    case (opts, batch) if batch.hasPut() || batch.hasDelete() =>
      Sync[F].delay {
        db.write(opts, batch)
        batch.clear()
        (opts, batch)
      }
    case unchanged =>
      unchanged.pure[F]
  }

  private def encode[T](value: T)(implicit ev: Codec[T]): F[Array[Byte]] =
    Sync[F].fromTry(ev.encode(value).map(_.toByteArray).toTry)

  private def decode[T](bytes: Array[Byte])(implicit ev: Codec[T]): F[T] =
    Sync[F].fromTry(ev.decodeValue(BitVector(bytes)).toTry)

  private def fromAutoCloseable[R <: AutoCloseable](mk: => R): Resource[F, R] =
    Resource.fromAutoCloseable[F, R](Sync[F].delay(mk))

  /** Execute a program that writes and potentially reads as well. */
  def update[A](program: KVStore[Namespace, A]): F[A] = {
    val compilerR = for {
      opts  <- fromAutoCloseable(new WriteOptions())
      batch <- fromAutoCloseable(new WriteBatch())
    } yield (opts, batch)

    compilerR.use { state =>
      program.foldMap(batchingCompiler).run(state).flatMap { case (state, a) =>
        executeBatch(state).as(a)
      }
    }
  }

  /** Execute a program that only has reads. */
  def query[A](program: KVStore[Namespace, A]): F[A] = ???
}

object RocksDBStore {
  type Namespace = IndexedSeq[Byte]

  /** Database operations may fail due to a couple of reasons:
    * - database connection issues
    * - obsolete format stored, codec unable to read data
    *
    * But it's not expected, so just using `F[A]` for now,
    * rather than `EitherT[F, Throwable, A]`.
    */
  type DBQuery[F[_], A] = F[A]

  def apply[F[_]: Sync](): Resource[F, RocksDBStore[F]] = ???
}
