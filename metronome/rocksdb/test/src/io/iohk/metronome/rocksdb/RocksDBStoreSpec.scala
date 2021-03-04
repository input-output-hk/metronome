package io.iohk.metronome.rocksdb

import cats.effect.Resource
import io.iohk.metronome.storage.KVStoreState
import java.nio.file.Files
import monix.eval.Task
import org.scalacheck.commands.Commands
import org.scalacheck.Gen
import org.scalacheck.Arbitrary.arbitrary
// import org.scalacheck.Prop
// import scala.util.{Try, Success}
import scala.concurrent.duration._
import scala.annotation.nowarn

// https://github.com/typelevel/scalacheck/blob/master/doc/UserGuide.md#stateful-testing
// https://github.com/typelevel/scalacheck/tree/master/examples

class RocksDBStoreSpec extends Commands {
  import RocksDBStore.Namespace

  object InMemoryKVS extends KVStoreState[Namespace]

  case class AllocatedDatabase(
      db: RocksDBStore[Task],
      release: Task[Unit]
  )

  case class Model(
      namespaces: Seq[Namespace],
      store: InMemoryKVS.Store
  )

  type State = Model

  type Sut = AllocatedDatabase

  private def await[T](task: Task[T]): T = {
    import monix.execution.Scheduler.Implicits.global
    task.runSyncUnsafe(timeout = 10.seconds)
  }

  /** Run one database at any time. */
  @nowarn // Traversable deprecated in 2.13
  override def canCreateNewSut(
      newState: State,
      initSuts: Traversable[State],
      runningSuts: Traversable[Sut]
  ): Boolean =
    initSuts.isEmpty && runningSuts.isEmpty

  /** Start with an empty database. */
  override def initialPreCondition(state: State): Boolean =
    state.store.isEmpty

  /** Create a new empty database. */
  override def newSut(state: State): Sut = {
    val res = for {
      path <- Resource.make(Task {
        Files.createTempDirectory("testdb").getFileName
      }) { path =>
        Task {
          if (Files.exists(path)) Files.delete(path)
        }
      }

      config = RocksDBStore.Config.default(path)

      _ <- Resource.make(Task.unit) { _ =>
        RocksDBStore.destroy[Task](config)
      }

      db <- RocksDBStore[Task](config, state.namespaces)
    } yield db

    await {
      res.allocated.map { case (db, release) =>
        AllocatedDatabase(db, release)
      }
    }
  }

  /** Release the database and all resources. */
  override def destroySut(sut: Sut): Unit =
    await(sut.release)

  /** Initialise a fresh model state. */
  override def genInitialState: Gen[State] =
    for {
      n  <- Gen.choose(1, 10)
      ns <- Gen.listOfN(n, arbitrary[Array[Byte]])
    } yield Model(ns.map(_.toIndexedSeq), Map.empty)

  /** Produce a Command based on the current model state. */
  def genCommand(state: State): Gen[Command] = ???
}
