package io.iohk.metronome.checkpointing.service.storage

import cats.implicits._
import io.iohk.metronome.core.Tagger
import io.iohk.metronome.checkpointing.models.Ledger
import io.iohk.metronome.checkpointing.models.ArbitraryInstances
import io.iohk.metronome.storage.{KVCollection, KVStoreState}
import org.scalacheck.{Properties, Gen, Arbitrary}, Arbitrary.arbitrary
import org.scalacheck.Prop.{forAll, all, propBoolean}
import scodec.Codec
import scodec.bits.BitVector
import org.scalacheck.Shrink
import scala.annotation.nowarn

object LedgerStorageProps extends Properties("LedgerStorage") {
  import ArbitraryInstances.arbLedger

  type Namespace = String
  object Namespace {
    val Ledgers    = "ledgers"
    val LedgerMeta = "ledger-meta"
  }

  /** The in-memory KVStoreState doesn't invoke the codecs. */
  implicit def neverUsedCodec[T] =
    Codec[T](
      (_: T) => sys.error("Didn't expect to encode."),
      (_: BitVector) => sys.error("Didn't expect to decode.")
    )

  object TestKVStore extends KVStoreState[Namespace]

  object HistorySize extends Tagger[Int] {
    @nowarn
    implicit val shrink: Shrink[HistorySize] = Shrink(s => Stream.empty)
    implicit val arb: Arbitrary[HistorySize] = Arbitrary {
      Gen.choose(1, 10).map(HistorySize(_))
    }
  }
  type HistorySize = HistorySize.Tagged

  property("buffer") = forAll(
    for {
      ledgers <- arbitrary[List[Ledger]]
      maxSize <- arbitrary[HistorySize]
    } yield (ledgers, maxSize)
  ) { case (ledgers, maxSize) =>
    val ledgerStorage = new LedgerStorage[Namespace](
      new KVCollection[Namespace, Ledger.Hash, Ledger](Namespace.Ledgers),
      Namespace.LedgerMeta,
      maxHistorySize = maxSize
    )

    val store =
      TestKVStore
        .compile(ledgers.traverse(ledgerStorage.put))
        .runS(Map.empty)
        .value

    def getByHash(ledgerHash: Ledger.Hash) =
      TestKVStore.compile(ledgerStorage.get(ledgerHash)).run(store)

    val ledgerMap      = store.get(Namespace.Ledgers).getOrElse(Map.empty[Any, Any])
    val (current, old) = ledgers.reverse.splitAt(maxSize)

    all(
      "max-history" |: ledgerMap.values.size <= maxSize,
      "contains current" |: current.forall { ledger =>
        getByHash(ledger.hash).contains(ledger)
      },
      "not contain old" |: old.forall { ledger =>
        getByHash(ledger.hash).isEmpty
      }
    )
  }
}
