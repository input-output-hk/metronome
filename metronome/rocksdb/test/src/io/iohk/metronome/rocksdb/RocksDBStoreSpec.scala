package io.iohk.metronome.rocksdb

import cats.implicits._
import cats.effect.Resource
import io.iohk.metronome.storage.{KVStoreState, KVStore}
import java.nio.file.Files
import monix.eval.Task
import org.scalacheck.commands.Commands
import org.scalacheck.{Properties, Gen, Prop, Test, Arbitrary}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.forAll
import scala.util.{Try, Success}
import scala.concurrent.duration._
import scala.annotation.nowarn
import io.iohk.metronome.storage.KVCollection
import scodec.bits.ByteVector
import scodec.codecs.implicits._

// https://github.com/typelevel/scalacheck/blob/master/doc/UserGuide.md#stateful-testing
// https://github.com/typelevel/scalacheck/blob/master/examples/commands-redis/src/test/scala/CommandsRedis.scala

object RocksDBStoreSpec extends Properties("RocksDBStoreCommands") {

  override def overrideParameters(p: Test.Parameters): Test.Parameters =
    p.withMinSuccessfulTests(10).withMaxSize(100)

  // Equivalent to the in-memory model.
  property("equivalent") = RocksDBStoreCommands.property()

  // Run two programs concurrently.
  property("linearizable") = forAll {
    import RocksDBStoreCommands._
    for {
      empty <- genInitialState
      // Generate some initial data. Puts are the only useful op.
      init <- Gen.listOfN(50, genPut(empty)).map { ops =>
        RunProgram(ops.toList.sequence, batching = true)
      }
      state = init.nextState(empty)
      // The first program is batching, it takes a write lock.
      prog1 <- genProgram(state).map(_.copy(batching = true))
      // The second program is not batching, it takes a read lock.
      prog2 <- genProgram(state).map(_.copy(batching = false))
    } yield (init, state, prog1, prog2)
  } { case (init, state, prog1, prog2) =>
    import RocksDBStoreCommands._

    val sut = newSut(state)
    try {
      // Connect to the database.
      ToggleConnected.run(sut)
      // Initialize the database.
      init.run(sut)

      // Run them concurrently. They should be serialised.
      val (result1, result2) = await {
        Task.parMap2(Task(prog1.run(sut)), Task(prog2.run(sut)))((_, _))
      }

      // Overall the results should correspond to either prog1 ++ prog2, or prog2 ++ prog1.
      val prog12 =
        RunProgram((prog1.program, prog2.program).mapN(_ ++ _))
      val prog21 =
        RunProgram((prog2.program, prog1.program).mapN(_ ++ _))

      // One of them should have run first.
      val prop1 = prog1.postCondition(state, Success(result1))
      val prop2 = prog2.postCondition(state, Success(result2))
      // The other should run second, on top of the changes from the first.
      val prop12 = prog12.postCondition(state, Success(result1 ++ result2))
      val prop21 = prog21.postCondition(state, Success(result2 ++ result1))

      (prop1 && prop12) || (prop2 && prop21)
    } finally {
      destroySut(sut)
    }
  }
}

object RocksDBStoreCommands extends Commands {
  import RocksDBStore.Namespace

  // The in-memory implementation is our reference execution model.
  object InMemoryKVS extends KVStoreState[Namespace]

  // Some structured data to be stored in the database.
  case class TestRecord(id: ByteVector, name: String, value: Int)

  // Symbolic state of the test.
  case class Model(
      // Support opening/closing the database to see if it can read back the files it has created.
      isConnected: Boolean,
      namespaces: IndexedSeq[Namespace],
      store: InMemoryKVS.Store,
      deleted: Map[Namespace, Set[Any]],
      // Some collections so we have typed access.
      coll0: KVCollection[Namespace, String, Int],
      coll1: KVCollection[Namespace, Int, ByteVector],
      coll2: KVCollection[Namespace, ByteVector, TestRecord]
  ) {

    def storeOf(coll: Coll): Map[Any, Any] =
      store.getOrElse(namespaces(coll.idx), Map.empty)

    def nonEmptyColls: List[Coll] =
      Colls.filter(c => storeOf(c).nonEmpty)
  }
  sealed trait Coll {
    def idx: Int
  }
  case object Coll0 extends Coll { def idx = 0 }
  case object Coll1 extends Coll { def idx = 1 }
  case object Coll2 extends Coll { def idx = 2 }

  val Colls = List(Coll0, Coll1, Coll2)

  case class Allocated[T](value: T, release: Task[Unit])

  class Database(
      val namespaces: Seq[Namespace],
      val config: Allocated[RocksDBStore.Config],
      var maybeConnection: Option[Allocated[RocksDBStore[Task]]]
  )

  type State = Model
  type Sut   = Database

  def await[T](task: Task[T]): T = {
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
      n  <- Gen.choose(3, 10)
      ns <- Gen.listOfN(n, arbitrary[Array[Byte]].suchThat(_.nonEmpty))
      namespaces = ns.map(_.toIndexedSeq).toIndexedSeq
    } yield Model(
      isConnected = false,
      namespaces = namespaces,
      store = Map.empty,
      deleted = Map.empty,
      coll0 = new KVCollection[Namespace, String, Int](namespaces(0)),
      coll1 = new KVCollection[Namespace, Int, ByteVector](namespaces(1)),
      coll2 = new KVCollection[Namespace, ByteVector, TestRecord](namespaces(2))
    )

  /** Produce a Command based on the current model state. */
  def genCommand(state: State): Gen[Command] =
    if (!state.isConnected) Gen.const(ToggleConnected)
    else
      Gen.frequency(
        (10, genProgram(state)),
        (1, Gen.const(ToggleConnected))
      )

  /** Generate a sequence of writes and reads. */
  def genProgram(state: State): Gen[RunProgram] =
    for {
      batching <- arbitrary[Boolean]
      n        <- Gen.choose(0, 20)
      ops <- Gen.listOfN(
        n,
        Gen.frequency(
          20 -> genPut(state),
          20 -> genPutExisting(state),
          5  -> genDel(state),
          15 -> genDelExisting(state),
          5  -> genGet(state),
          30 -> genGetExisting(state),
          5  -> genGetDeleted(state)
        )
      )
      program = ops.toList.sequence
    } yield RunProgram(program, batching)

  implicit val arbColl: Arbitrary[Coll] = Arbitrary {
    Gen.oneOf(Coll0, Coll1, Coll2)
  }

  implicit val arbByteVector: Arbitrary[ByteVector] = Arbitrary {
    arbitrary[Array[Byte]].map(ByteVector(_))
  }

  implicit val arbTestRecord: Arbitrary[TestRecord] = Arbitrary {
    for {
      id    <- arbitrary[ByteVector]
      name  <- Gen.alphaNumStr
      value <- arbitrary[Int]
    } yield TestRecord(id, name, value)
  }

  def genPut(state: State): Gen[KVStore[Namespace, Any]] =
    arbitrary[Coll] flatMap {
      case Coll0 =>
        for {
          k <- Gen.alphaLowerStr.suchThat(_.nonEmpty)
          v <- arbitrary[Int]
        } yield state.coll0.put(k, v)

      case Coll1 =>
        for {
          k <- arbitrary[Int]
          v <- arbitrary[ByteVector]
        } yield state.coll1.put(k, v)

      case Coll2 =>
        for {
          k <- arbitrary[ByteVector].suchThat(_.nonEmpty)
          v <- arbitrary[TestRecord]
        } yield state.coll2.put(k, v)
    } map {
      _.map(_.asInstanceOf[Any])
    }

  def genPutExisting(state: State): Gen[KVStore[Namespace, Any]] =
    state.nonEmptyColls match {
      case Nil =>
        genPut(state)

      case colls =>
        for {
          c <- Gen.oneOf(colls)
          k <- Gen.oneOf(state.storeOf(c).keySet)
          op <- c match {
            case Coll0 =>
              arbitrary[Int].map { v =>
                state.coll0.put(k.asInstanceOf[String], v)
              }
            case Coll1 =>
              arbitrary[ByteVector].map { v =>
                state.coll1.put(k.asInstanceOf[Int], v)
              }
            case Coll2 =>
              arbitrary[TestRecord].map { v =>
                state.coll2.put(k.asInstanceOf[ByteVector], v)
              }
          }
        } yield op.map(_.asInstanceOf[Any])
    }

  def genDel(state: State): Gen[KVStore[Namespace, Any]] =
    arbitrary[Coll] flatMap {
      case Coll0 =>
        arbitrary[String].map(state.coll0.delete)
      case Coll1 =>
        arbitrary[Int].map(state.coll1.delete)
      case Coll2 =>
        arbitrary[ByteVector].map(state.coll2.delete)
    } map {
      _.map(_.asInstanceOf[Any])
    }

  def genDelExisting(state: State): Gen[KVStore[Namespace, Any]] =
    state.nonEmptyColls match {
      case Nil =>
        genGet(state)

      case colls =>
        for {
          c <- Gen.oneOf(colls)
          k <- Gen.oneOf(state.storeOf(c).keySet)
          op = c match {
            case Coll0 =>
              state.coll0.delete(k.asInstanceOf[String])
            case Coll1 =>
              state.coll1.delete(k.asInstanceOf[Int])
            case Coll2 =>
              state.coll2.delete(k.asInstanceOf[ByteVector])
          }
        } yield op.map(_.asInstanceOf[Any])
    }

  def genGet(state: State): Gen[KVStore[Namespace, Any]] =
    arbitrary[Coll] flatMap {
      case Coll0 =>
        arbitrary[String].map(state.coll0.get)
      case Coll1 =>
        arbitrary[Int].map(state.coll1.get)
      case Coll2 =>
        arbitrary[ByteVector].map(state.coll2.get)
    } map {
      _.map(_.asInstanceOf[Any])
    }

  def genGetExisting(state: State): Gen[KVStore[Namespace, Any]] =
    state.nonEmptyColls match {
      case Nil =>
        genGet(state)

      case colls =>
        for {
          c <- Gen.oneOf(colls)
          k <- Gen.oneOf(state.storeOf(c).keySet)
          op = c match {
            case Coll0 =>
              state.coll0.get(k.asInstanceOf[String])
            case Coll1 =>
              state.coll1.get(k.asInstanceOf[Int])
            case Coll2 =>
              state.coll2.get(k.asInstanceOf[ByteVector])
          }
        } yield op.map(_.asInstanceOf[Any])
    }

  def genGetDeleted(state: State): Gen[KVStore[Namespace, Any]] = {
    val hasDeletes =
      Colls
        .map { c =>
          c -> state.namespaces(c.idx)
        }
        .filter { case (_, n) =>
          state.deleted.getOrElse(n, Set.empty).nonEmpty
        }

    hasDeletes match {
      case Nil =>
        genGet(state)

      case deletes =>
        for {
          cn <- Gen.oneOf(deletes)
          (c, n) = cn
          k <- Gen.oneOf(state.deleted(n))
          op = c match {
            case Coll0 =>
              state.coll0.get(k.asInstanceOf[String])
            case Coll1 =>
              state.coll1.get(k.asInstanceOf[Int])
            case Coll2 =>
              state.coll2.get(k.asInstanceOf[ByteVector])
          }
        } yield op.map(_.asInstanceOf[Any])
    }
  }

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

  case class RunProgram(
      program: KVStore[Namespace, List[Any]],
      batching: Boolean = false
  ) extends Command {
    // Collect all results from a batch of execution steps.
    type Result = List[Any]

    def run(sut: Sut): Result = {
      val db = sut.maybeConnection
        .getOrElse(sys.error("The database is not connected."))
        .value

      await {
        if (batching) {
          db.runWithBatching(program)
        } else {
          db.runWithoutBatching(program)
        }
      }
    }

    def preCondition(state: State): Boolean = state.isConnected

    def nextState(state: State): State = {
      val nextStore = InMemoryKVS.compile(program).runS(state.store).value

      // Leave only what's still deleted. Add what's been deleted now.
      val nextDeleted = state.deleted.map { case (n, ks) =>
        val existing = nextStore.getOrElse(n, Map.empty).keySet
        n -> ks.filterNot(existing)
      } ++ state.store.map { case (n, kvs) =>
        val existing = nextStore.getOrElse(n, Map.empty).keySet
        n -> (kvs.keySet -- existing)
      }

      state.copy(
        store = nextStore,
        deleted = nextDeleted
      )
    }

    def postCondition(state: Model, result: Try[Result]): Prop = {
      val expected = InMemoryKVS.compile(program).runA(state.store).value
      result == Success(expected)
    }
  }
}
