package io.iohk.metronome.storage

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scodec.codecs.implicits._

class KVStoreStateSpec extends AnyFlatSpec with Matchers {
  import KVStoreStateSpec._

  behavior of "KVStoreState"

  it should "compose multiple collections" in {
    type Namespace = String
    // Two independent collections with different types of keys and values.
    val collA = new KVCollection[Namespace, Int, RecordA](namespace = "a")
    val collB = new KVCollection[Namespace, String, RecordB](namespace = "b")

    val program: KVStore[Namespace, Option[RecordA]] = for {
      _ <- collA.put(1, RecordA("one"))
      _ <- collB.put("two", RecordB(2))
      b <- collB.get("three")
      _ <- collB.put("three", RecordB(3))
      _ <- collB.delete("two")
      _ <-
        if (b.isEmpty) collA.put(4, RecordA("four"))
        else KVStore.unit[Namespace]
      a <- collA.read(1).lift
    } yield a

    val compiler = new KVStoreState[Namespace]

    val (store, maybeA) = compiler.compile(program).run(Map.empty).value

    maybeA shouldBe Some(RecordA("one"))
    store shouldBe Map(
      "a" -> Map(1 -> RecordA("one"), 4 -> RecordA("four")),
      "b" -> Map("three" -> RecordB(3))
    )
  }
}

object KVStoreStateSpec {
  case class RecordA(a: String)
  case class RecordB(b: Int)
}
