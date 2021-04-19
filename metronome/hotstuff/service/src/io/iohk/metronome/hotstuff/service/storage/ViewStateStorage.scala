package io.iohk.metronome.hotstuff.service.storage

import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.{Agreement, QuorumCertificate}
import io.iohk.metronome.storage.KVStore
import scodec.Codec

/** Storing elements of the view state individually under separate keys,
  * because they get written independently.
  */
class ViewStateStorage[N, A <: Agreement](namespace: N)(implicit
    kvn: KVStore.Ops[N],
    codecVN: Codec[ViewNumber],
    codecQC: Codec[QuorumCertificate[A]],
    codecH: Codec[A#Hash]
) {}

object ViewStateStorage {

  sealed trait Key
  object Key {
    case object ViewNumber            extends Key
    case object PrepareQC             extends Key
    case object LockedQC              extends Key
    case object CommitQC              extends Key
    case object LastExecutedBlockHash extends Key
  }

  /** The state of consensus that needs to be persisted between restarts.
    *
    * The fields are a subset of the `ProtocolState` but have a slightly
    * different life cylce, e.g. `lastExecutedBlockHash` is only updated
    * when the blocks are actually executed, which happens asynchronously.
    */
  case class Bundle[A <: Agreement](
      viewNumber: ViewNumber,
      prepareQC: QuorumCertificate[A],
      lockedQC: QuorumCertificate[A],
      commitQC: QuorumCertificate[A],
      lastExecutedBlockHash: A#Hash
  )

  /** Create a ViewStateStorage instance by pre-loading it with the genesis,
    * unless it already has data.
    */
  def apply[N, A <: Agreement](
      namespace: N,
      genesisQC: QuorumCertificate[A]
  )(implicit
      codecVN: Codec[ViewNumber],
      codecQC: Codec[QuorumCertificate[A]],
      codecH: Codec[A#Hash]
  ): KVStore[N, ViewStateStorage[N, A]] = {
    implicit val kvn = KVStore.instance[N]

    def setDefault[V](default: V): Option[V] => Option[V] =
      (current: Option[V]) => current orElse Some(default)

    for {
      _ <- KVStore[N].alter(namespace, Key.ViewNumber)(
        setDefault(genesisQC.viewNumber)
      )
      _ <- KVStore[N].alter(namespace, Key.PrepareQC)(setDefault(genesisQC))
      _ <- KVStore[N].alter(namespace, Key.LockedQC)(setDefault(genesisQC))
      _ <- KVStore[N].alter(namespace, Key.CommitQC)(setDefault(genesisQC))
      _ <- KVStore[N].alter(namespace, Key.LastExecutedBlockHash)(
        setDefault(genesisQC.blockHash)
      )
    } yield new ViewStateStorage[N, A](namespace)
  }

}
