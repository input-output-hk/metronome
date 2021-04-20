package io.iohk.metronome.rocksdb

import cats._
import cats.implicits._
import cats.data.ReaderT
import cats.effect.{Resource, Sync}
import cats.free.Free.liftF
import io.iohk.metronome.storage.{
  KVStore,
  KVStoreOp,
  KVStoreRead,
  KVStoreReadOp
}
import io.iohk.metronome.storage.KVStoreOp.{Put, Get, Delete}
import java.util.concurrent.locks.ReentrantReadWriteLock
import org.rocksdb.{
  RocksDB,
  WriteBatch,
  WriteOptions,
  ReadOptions,
  Options,
  DBOptions,
  ColumnFamilyOptions,
  ColumnFamilyDescriptor,
  ColumnFamilyHandle,
  BlockBasedTableConfig,
  BloomFilter,
  CompressionType,
  ClockCache
}
import scodec.{Encoder, Decoder}
import scodec.bits.BitVector
import scala.collection.mutable
import java.nio.file.Path
import scala.annotation.nowarn

/** Implementation of intepreters for `KVStore[N, A]` and `KVStoreRead[N, A]`operations
  * with various semantics. Application code is not expected to interact with this class
  * directly. Instead, some middle layer should be passed as a dependency to code that
  * delegates to the right interpreter in this class.
  *
  * For example if our data schema is append-only, there's no need to pay the performance
  * penalty for using locking, or if two parts of the application are isolated from each other,
  * locking could be performed in their respective middle-layers, before they forward the
  * query for execution to this class.
  */
class RocksDBStore[F[_]: Sync](
    db: RocksDBStore.DBSupport[F],
    lock: RocksDBStore.LockSupport[F],
    handles: Map[RocksDBStore.Namespace, ColumnFamilyHandle]
) {

  import RocksDBStore.{Namespace, DBQuery, autoCloseableR}

  private val kvs = KVStore.instance[Namespace]

  // Batch execution needs these variables for accumulating operations
  // and executing them against the database. They are going to be
  // passed along in a Reader monad to the Free compiler.
  type BatchEnv = WriteBatch

  // Type aliases to support the `~>` transformation with types that
  // only have 1 generic type argument `A`.
  type Batch[A] =
    ({ type L[A] = ReaderT[F, BatchEnv, A] })#L[A]

  type KVNamespacedOp[A] =
    ({ type L[A] = KVStoreOp[Namespace, A] })#L[A]

  type KVNamespacedReadOp[A] =
    ({ type L[A] = KVStoreReadOp[Namespace, A] })#L[A]

  /** Execute the accumulated write operations in a batch. */
  private val writeBatch: ReaderT[F, BatchEnv, Unit] =
    ReaderT { batch =>
      if (batch.hasPut() || batch.hasDelete())
        db.write(batch) >>
          Sync[F].delay {
            batch.clear()
          }
      else
        ().pure[F]
    }

  /** Execute one `Get` operation. */
  private def read[K, V](op: Get[Namespace, K, V]): F[Option[V]] = {
    for {
      kbs  <- encode(op.key)(op.keyEncoder)
      mvbs <- db.read(handles(op.namespace), kbs)
      mv <- mvbs match {
        case None =>
          none.pure[F]

        case Some(bytes) =>
          decode(bytes)(op.valueDecoder).map(_.some)
      }
    } yield mv
  }

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
            ReaderT { batch =>
              for {
                kbs <- encode(k)(op.keyEncoder)
                vbs <- encode(v)(op.valueEncoder)
                _ = batch.put(handles(n), kbs, vbs)
              } yield ()
            }

          case op @ Get(_, _) =>
            // Execute any pending deletes and puts before performing the read.
            writeBatch >> ReaderT.liftF(read(op))

          case op @ Delete(n, k) =>
            ReaderT { batch =>
              for {
                kbs <- encode(k)(op.keyEncoder)
                _ = batch.delete(handles(n), kbs)
              } yield ()
            }
        }
    }

  /** Intended for reads, with fallback to writes. */
  private val nonBatchingCompiler: KVNamespacedOp ~> F =
    new (KVNamespacedOp ~> F) {
      def apply[A](fa: KVNamespacedOp[A]): F[A] =
        fa match {
          case op @ Get(_, _) =>
            read(op)
          case op =>
            lock.withLockUpgrade {
              runWithBatchingNoLock {
                liftF[KVNamespacedOp, A](op)
              }
            }
        }
    }

  private def encode[T](value: T)(implicit ev: Encoder[T]): F[Array[Byte]] =
    Sync[F].fromTry(ev.encode(value).map(_.toByteArray).toTry)

  private def decode[T](bytes: Array[Byte])(implicit ev: Decoder[T]): F[T] =
    Sync[F].fromTry(ev.decodeValue(BitVector(bytes)).toTry)

  /** Mostly meant for writing batches atomically.
    *
    * If a read is found the accumulated writes are performed,
    * then the read happens, before batching carries on;
    * this breaks the atomicity of writes.
    *
    * This version doesn't use any locking, so it's suitable for
    * append-only data stores, or writing to independent stores
    * in parallel.
    */
  def runWithBatchingNoLock[A](
      program: KVStore[Namespace, A]
  ): DBQuery[F, A] = {
    autoCloseableR(new WriteBatch()).use {
      (program.foldMap(batchingCompiler) <* writeBatch).run
    }
  }

  /** Same as `runWithBatchingNoLock`, but write lock is taken out
    * to make sure concurrent reads are not affected.
    *
    * This version is suitable for cases where data may be deleted,
    * which could result for example in foreign key references
    * becoming invalid after they are read, before the data they
    * point to is retrieved.
    */
  def runWithBatching[A](program: KVStore[Namespace, A]): DBQuery[F, A] =
    lock.withWriteLock {
      runWithBatchingNoLock(program)
    }

  /** Similar to `runWithBatching` in that it can contain both reads
    * and writes, but the expectation is that it will mostly be reads.
    *
    * A read lock is taken out to make sure writes don't affect reads;
    * if a write is found, it is executed as an individual operation,
    * while a write lock is taken out to protect other reads. Note that
    * this breaks the isolation of reads, because to acquire a write lock,
    * the read lock has to be released, which gives a chance for other
    * threads to get in before the write statement runs.
    */
  def runWithoutBatching[A](program: KVStore[Namespace, A]): DBQuery[F, A] =
    lock.withReadLock {
      program.foldMap(nonBatchingCompiler)
    }

  /** For strictly read-only operations.
    *
    * Doesn't use locking, so most suitable for append-only data schemas
    * where reads don't need isolation from writes.
    */
  def runReadOnlyNoLock[A](program: KVStoreRead[Namespace, A]): DBQuery[F, A] =
    kvs.lift(program).foldMap(nonBatchingCompiler)

  /** Same as `runReadOnlyNoLock`, but a read lock is taken out
    * to make sure concurrent writes cannot affect the results.
    *
    * This version is suitable for use cases where destructive
    * updates are happening.
    */
  def runReadOnly[A](program: KVStoreRead[Namespace, A]): DBQuery[F, A] =
    lock.withReadLock {
      runReadOnlyNoLock(program)
    }
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

  case class Config(
      path: Path,
      createIfMissing: Boolean,
      paranoidChecks: Boolean,
      maxThreads: Int,
      maxOpenFiles: Int,
      verifyChecksums: Boolean,
      levelCompaction: Boolean,
      blockSizeBytes: Long,
      blockCacheSizeBytes: Long
  )
  object Config {
    def default(path: Path): Config =
      Config(
        path = path,
        // Create DB data directory if it's missing
        createIfMissing = true,
        // Should the DB raise an error as soon as it detects an internal corruption
        paranoidChecks = true,
        maxThreads = 1,
        maxOpenFiles = 32,
        // Force checksum verification of all data that is read from the file system on behalf of a particular read.
        verifyChecksums = true,
        // In this mode, size target of levels are changed dynamically based on size of the last level.
        // https://rocksdb.org/blog/2015/07/23/dynamic-level.html
        levelCompaction = true,
        // Approximate size of user data packed per block (16 * 1024)
        blockSizeBytes = 16384,
        // Amount of cache in bytes that will be used by RocksDB (32 * 1024 * 1024)
        blockCacheSizeBytes = 33554432
      )
  }

  def apply[F[_]: Sync](
      config: Config,
      namespaces: Seq[Namespace]
  ): Resource[F, RocksDBStore[F]] = {

    @nowarn // JavaConverters are deprecated in 2.13
    def open(
        opts: DBOptions,
        cfds: Seq[ColumnFamilyDescriptor],
        cfhs: mutable.Buffer[ColumnFamilyHandle]
    ): RocksDB = {
      import scala.collection.JavaConverters._
      RocksDB.open(opts, config.path.toString, cfds.asJava, cfhs.asJava)
    }

    // There is a specific order for closing RocksDB with column families described in
    // https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
    // 1. Free all column families handles
    // 2. Free DB and DB options
    // 3. Free column families options
    // So they are created in the opposite order.
    for {
      _ <- Resource.liftF[F, Unit](Sync[F].delay {
        RocksDB.loadLibrary()
      })

      tableConf <- Resource.pure[F, BlockBasedTableConfig] {
        mkTableConfig(config)
      }

      cfOpts <- autoCloseableR[F, ColumnFamilyOptions] {
        new ColumnFamilyOptions()
          .setCompressionType(CompressionType.LZ4_COMPRESSION)
          .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
          .setLevelCompactionDynamicLevelBytes(config.levelCompaction)
          .setTableFormatConfig(tableConf)
      }

      allNamespaces = RocksDB.DEFAULT_COLUMN_FAMILY.toIndexedSeq +: namespaces

      cfDescriptors = allNamespaces.map { n =>
        new ColumnFamilyDescriptor(n.toArray, cfOpts)
      }

      dbOpts <- autoCloseableR[F, DBOptions] {
        new DBOptions()
          .setCreateIfMissing(config.createIfMissing)
          .setParanoidChecks(config.paranoidChecks)
          .setMaxOpenFiles(config.maxOpenFiles)
          .setIncreaseParallelism(config.maxThreads)
          .setCreateMissingColumnFamilies(true)
      }

      readOpts <- autoCloseableR[F, ReadOptions] {
        new ReadOptions().setVerifyChecksums(config.verifyChecksums)
      }
      writeOptions <- autoCloseableR[F, WriteOptions] {
        new WriteOptions()
      }

      // The handles will be filled as the database is opened.
      columnFamilyHandleBuffer = mutable.Buffer.empty[ColumnFamilyHandle]

      db <- autoCloseableR[F, RocksDB] {
        open(
          dbOpts,
          cfDescriptors,
          columnFamilyHandleBuffer
        )
      }

      columnFamilyHandles <- Resource.make(
        (allNamespaces zip columnFamilyHandleBuffer).toMap.pure[F]
      ) { _ =>
        // Make sure all handles are closed, and this happens before the DB is closed.
        Sync[F].delay(columnFamilyHandleBuffer.foreach(_.close()))
      }

      // Sanity check; if an exception is raised everything will be closed down.
      _ = assert(
        columnFamilyHandleBuffer.size == allNamespaces.size,
        "Should have created a column family handle for each namespace." +
          s" Expected ${allNamespaces.size}; got ${columnFamilyHandleBuffer.size}."
      )

      store = new RocksDBStore[F](
        new DBSupport(db, readOpts, writeOptions),
        new LockSupport(new ReentrantReadWriteLock()),
        columnFamilyHandles
      )

    } yield store
  }

  /** Remove the database directory. */
  def destroy[F[_]: Sync](
      config: Config
  ): F[Unit] = {
    autoCloseableR[F, Options] {
      new Options()
        .setCreateIfMissing(config.createIfMissing)
        .setParanoidChecks(config.paranoidChecks)
        .setCompressionType(CompressionType.LZ4_COMPRESSION)
        .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
        .setLevelCompactionDynamicLevelBytes(config.levelCompaction)
        .setMaxOpenFiles(config.maxOpenFiles)
        .setIncreaseParallelism(config.maxThreads)
        .setTableFormatConfig(mkTableConfig(config))
    }.use { options =>
      Sync[F].delay {
        RocksDB.destroyDB(config.path.toString, options)
      }
    }
  }

  private def mkTableConfig(config: Config): BlockBasedTableConfig =
    new BlockBasedTableConfig()
      .setBlockSize(config.blockSizeBytes)
      .setBlockCache(new ClockCache(config.blockCacheSizeBytes))
      .setCacheIndexAndFilterBlocks(true)
      .setPinL0FilterAndIndexBlocksInCache(true)
      .setFilterPolicy(new BloomFilter(10, false))

  private def autoCloseableR[F[_]: Sync, R <: AutoCloseable](
      mk: => R
  ): Resource[F, R] =
    Resource.fromAutoCloseable[F, R](Sync[F].delay(mk))

  /** Help run reads and writes isolated from each other. */
  private class LockSupport[F[_]: Sync](rwlock: ReentrantReadWriteLock) {

    // Batches can interleave multiple reads (and writes);
    // to make sure they see a consistent view, writes are
    // isolated from reads via locks, so for example if we
    // read an ID, then retrieve the record from a different
    // collection, we can be sure it hasn't been deleted in
    // between the two operations.
    private val lockRead    = Sync[F].delay(rwlock.readLock().lock())
    private val unlockRead  = Sync[F].delay(rwlock.readLock().unlock())
    private val lockWrite   = Sync[F].delay(rwlock.writeLock().lock())
    private val unlockWrite = Sync[F].delay(rwlock.writeLock().unlock())

    def withReadLock[A](fa: F[A]): F[A] =
      Sync[F].bracket(lockRead)(_ => fa)(_ => unlockRead)

    def withWriteLock[A](fa: F[A]): F[A] =
      Sync[F].bracket(lockWrite)(_ => fa)(_ => unlockWrite)

    /*
     * In case there's a write operation among the reads and we haven't
     * taken out a write lock, we can replace the the read lock we have
     * with a write lock, for the duration of the operation, then downgrade
     * it back to when we're done.
     *
     * Note that *technically* this is not an upgrade: to acquire the write
     * lock, the read lock has to be released first, therefore other threads
     * may get the write lock first. It works in the other direction though:
     * the write lock can be turned into a read.
     *
     * See here for the rules up (non-)upgrading and downgrading:
     * https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/locks/ReentrantReadWriteLock.html
     */
    def withLockUpgrade[A](fa: F[A]): F[A] =
      Sync[F].bracket {
        unlockRead >> lockWrite
      }(_ => fa) { _ =>
        lockRead >> unlockWrite
      }
  }

  /** Wrap a RocksDB instance. */
  private class DBSupport[F[_]: Sync](
      db: RocksDB,
      readOptions: ReadOptions,
      writeOptions: WriteOptions
  ) {
    def read(
        handle: ColumnFamilyHandle,
        key: Array[Byte]
    ): F[Option[Array[Byte]]] = Sync[F].delay {
      Option(db.get(handle, readOptions, key))
    }

    def write(
        batch: WriteBatch
    ): F[Unit] = Sync[F].delay {
      db.write(writeOptions, batch)
    }
  }
}
