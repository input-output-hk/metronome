package io.iohk.metronome.checkpointing.service.storage

import cats.implicits._
import io.iohk.metronome.checkpointing.models.Ledger
import io.iohk.metronome.storage.{KVRingBuffer, KVCollection, KVStore}
import scodec.Codec

/** Storing the committed and executed checkpoint ledger.
  *
  * Strictly speaking the application only needs the committed state,
  * since it has been signed by the federation and we know it's not
  * going to be rolled back. Uncommitted state can be kept in memory.
  *
  * However we want to support other nodes catching up by:
  * 1. requesting the latest Commit Q.C., then
  * 2. requesting the block the Commit Q.C. points at, then
  * 3. requesting the ledger state the header points at.
  *
  * We have to allow some time before we get rid of historical state,
  * so that it doesn't disappear between step 2 and 3, resulting in
  * nodes trying and trying to catch up but always missing the beat.
  *
  * Therefore we keep a collection of the last N ledgers in a ring buffer.
  */
class LedgerStorage[N](
    ledgerColl: KVCollection[N, Ledger.Hash, Ledger],
    ledgerMetaNamespace: N,
    maxHistorySize: Int
)(implicit codecH: Codec[Ledger.Hash])
    extends KVRingBuffer[N, Ledger.Hash, Ledger](
      ledgerColl,
      ledgerMetaNamespace,
      maxHistorySize
    ) {

  /** Save a new ledger and remove the oldest one, if we reached
    * the maximum history size. Since we only store committed
    * state, they form a chain. They will always be retrieved
    * by going through a block pointing at them directly.
    */
  def put(ledger: Ledger): KVStore[N, Unit] =
    put(ledger.hash, ledger).void
}
