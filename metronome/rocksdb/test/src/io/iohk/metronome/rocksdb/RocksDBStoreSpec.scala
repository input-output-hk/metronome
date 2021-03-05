package io.iohk.metronome.rocksdb

import cats.effect.Resource
import io.iohk.metronome.storage.KVStoreState
import java.nio.file.Files
import monix.eval.Task
import org.scalacheck.commands.Commands
import org.scalacheck.{Properties, Gen}
import org.scalacheck.Arbitrary.arbitrary
// import org.scalacheck.Prop
// import scala.util.{Try, Success}
import scala.concurrent.duration._
import scala.annotation.nowarn
import org.scalacheck.Test

// https://github.com/typelevel/scalacheck/blob/master/doc/UserGuide.md#stateful-testing
// https://github.com/typelevel/scalacheck/blob/master/examples/commands-redis/src/test/scala/CommandsRedis.scala

object RocksDBStoreSpec extends Properties("RocksDBStoreCommands") {

  override def overrideParameters(p: Test.Parameters): Test.Parameters =
    p.withMinSuccessfulTests(5).withMaxSize(100)

  property("RocksDBStoreSpec") = RocksDBStoreCommands.property()

}

object RocksDBStoreCommands extends Commands {
  import RocksDBStore.Namespace

  // The in-memory implementation is our reference execution model.
  object InMemoryKVS extends KVStoreState[Namespace]

  // Symbolic state of the test.
  case class Model(
      // Support opening/closing the database to see if it can read back the files it has created.
      isConnected: Boolean,
      namespaces: Seq[Namespace],
      store: InMemoryKVS.Store
  )

  case class Allocated[T](value: T, release: Task[Unit])

  class Database(
      val namespaces: Seq[Namespace],
      val config: Allocated[RocksDBStore.Config],
      var maybeConnection: Option[Allocated[RocksDBStore[Task]]]
  )

  type State = Model
  type Sut   = Database

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
    state.store.isEmpty && !state.isConnected

  /** Create a new empty database. */
  override def newSut(state: State): Sut = {
    val res = for {
      path <- Resource.make(Task {
        Files.createTempDirectory("testdb")
      }) { path =>
        Task {
          if (Files.exists(path)) Files.delete(path)
        }
      }

      config = RocksDBStore.Config.default(path)

      _ <- Resource.make(Task.unit) { _ =>
        RocksDBStore.destroy[Task](config)
      }
    } yield config

    await {
      res.allocated.map { case (config, release) =>
        new Database(
          state.namespaces,
          Allocated(config, release),
          maybeConnection = None
        )
      }
    }
  }

  /** Release the database and all resources. */
  override def destroySut(sut: Sut): Unit =
    await {
      sut.maybeConnection
        .fold(Task.unit)(_.release)
        .guarantee(sut.config.release)
    }

  /** Initialise a fresh model state. */
  override def genInitialState: Gen[State] =
    for {
      n  <- Gen.choose(1, 10)
      ns <- Gen.listOfN(n, arbitrary[Array[Byte]])
    } yield Model(
      isConnected = false,
      namespaces = ns.map(_.toIndexedSeq),
      store = Map.empty
    )

  /** Produce a Command based on the current model state. */
  def genCommand(state: State): Gen[Command] =
    if (!state.isConnected) Gen.const(ToggleConnected)
    else
      Gen.frequency(
        (1, Gen.const(ToggleConnected))
      )

  /** Open or close the database. */
  case object ToggleConnected extends UnitCommand {
    def run(sut: Sut) = {
      sut.maybeConnection match {
        case Some(connection) =>
          await(connection.release)
          sut.maybeConnection = None

        case None =>
          val connection = await {
            RocksDBStore[Task](sut.config.value, sut.namespaces).allocated
              .map { case (db, release) =>
                Allocated(db, release)
              }
          }
          sut.maybeConnection = Some(connection)
      }
    }

    def preCondition(state: State) = true
    def nextState(state: State) = state.copy(
      isConnected = !state.isConnected
    )
    def postCondition(state: State, succeeded: Boolean) = succeeded
  }
}
