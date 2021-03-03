package io.iohk.metronome.rocksdb

import cats._
import cats.implicits._
import cats.data.ReaderT
import cats.effect.{Resource, Sync}
import io.iohk.metronome.storage.{KVStore, KVStoreOp}
import io.iohk.metronome.storage.KVStoreOp.{Put, Get, Delete}
import org.rocksdb.{
  RocksDB,
  WriteBatch,
  ColumnFamilyHandle,
  WriteOptions,
  ReadOptions
}
import scodec.Codec
import scodec.bits.BitVector

class RocksDBStore[F[_]: Sync](
    db: RocksDB,
    handles: Map[RocksDBStore.Namespace, ColumnFamilyHandle],
    readOptions: ReadOptions
) {
  import RocksDBStore.{Namespace, DBQuery}

  type BatchEnv          = (WriteOptions, WriteBatch)
  type Batch[A]          = ({ type L[A] = ReaderT[F, BatchEnv, A] })#L[A]
  type KVNamespacedOp[A] = ({ type L[A] = KVStoreOp[Namespace, A] })#L[A]

  /** Collect writes in a batch, until we either get to the end, or there's a read.
    * This way writes are atomic, and reads can see their effect.
    *
    * In the next version of RocksDB we can use transactions to make reads and writes
    * run in isolation.
    */
  private val batchingCompiler: KVNamespacedOp ~> Batch =
    new (KVNamespacedOp ~> Batch) {
      def apply[A](fa: KVNamespacedOp[A]): Batch[A] =
        fa match {
          case op @ Put(n, k, v) =>
            ReaderT { case (_, batch) =>
              for {
                kbs <- encode(k)(op.keyCodec)
                vbs <- encode(v)(op.valueCodec)
                _ = batch.put(handles(n), kbs, vbs)
              } yield ()
            }

          case op @ Get(n, k) =>
            val getV: ReaderT[F, BatchEnv, A] = ReaderT.liftF[F, BatchEnv, A] {
              for {
                kbs <- encode(k)(op.keyCodec)
                mvbs <- Sync[F].delay[Option[Array[Byte]]] {
                  Option(db.get(handles(n), kbs))
                }
                mv <- mvbs match {
                  case None =>
                    none.pure[F]

                  case Some(bytes) =>
                    decode(bytes)(op.valueCodec).map(_.some)
                }
              } yield mv
            }

            // Execute any pending deletes and puts before performing the read.
            writeBatch >> getV

          case op @ Delete(n, k) =>
            ReaderT { case (_, batch) =>
              for {
                kbs <- encode(k)(op.keyCodec)
                _ = batch.delete(handles(n), kbs)
              } yield ()
            }
        }
    }

  private val writeBatch: ReaderT[F, BatchEnv, Unit] =
    ReaderT {
      case (opts, batch) if batch.hasPut() || batch.hasDelete() =>
        Sync[F].delay {
          db.write(opts, batch)
          batch.clear()
        }
      case _ =>
        ().pure[F]
    }

  private def encode[T](value: T)(implicit ev: Codec[T]): F[Array[Byte]] =
    Sync[F].fromTry(ev.encode(value).map(_.toByteArray).toTry)

  private def decode[T](bytes: Array[Byte])(implicit ev: Codec[T]): F[T] =
    Sync[F].fromTry(ev.decodeValue(BitVector(bytes)).toTry)

  private def fromAutoCloseable[R <: AutoCloseable](mk: => R): Resource[F, R] =
    Resource.fromAutoCloseable[F, R](Sync[F].delay(mk))

  /** Execute a program that writes and potentially reads as well. */
  def update[A](program: KVStore[Namespace, A]): DBQuery[F, A] = {
    val env = for {
      opts  <- fromAutoCloseable(new WriteOptions())
      batch <- fromAutoCloseable(new WriteBatch())
    } yield (opts, batch)

    env.use {
      (program.foldMap(batchingCompiler) <* writeBatch).run
    }
  }

  /** Execute a program that only has reads.
    * If a write is found, the program switches to `update`.
    */
  def read[A](program: KVStore[Namespace, A]): DBQuery[F, A] = ???
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
