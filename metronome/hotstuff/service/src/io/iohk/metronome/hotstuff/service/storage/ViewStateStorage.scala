package io.iohk.metronome.hotstuff.service.storage

import cats.implicits._
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  QuorumCertificate,
  Phase
}
import io.iohk.metronome.storage.{KVStore, KVStoreRead}
import scodec.{Codec, Encoder, Decoder}

class ViewStateStorage[N, A <: Agreement] private (
    namespace: N
)(implicit
    keys: ViewStateStorage.Keys[A],
    kvn: KVStore.Ops[N],
    kvrn: KVStoreRead.Ops[N],
    codecVN: Codec[ViewNumber],
    codecQC: Codec[QuorumCertificate[A]],
    codecH: Codec[A#Hash]
) {
  import keys.Key

  private def put[V: Encoder](key: Key[V], value: V) =
    KVStore[N].put[Key[V], V](namespace, key, value)

  private def read[V: Decoder](key: Key[V]): KVStoreRead[N, V] =
    KVStoreRead[N].read[Key[V], V](namespace, key).map(_.get)

  def setViewNumber(viewNumber: ViewNumber): KVStore[N, Unit] =
    put(Key.ViewNumber, viewNumber)

  def setQuorumCertificate(qc: QuorumCertificate[A]): KVStore[N, Unit] =
    qc.phase match {
      case Phase.Prepare =>
        put(Key.PrepareQC, qc)
      case Phase.PreCommit =>
        put(Key.LockedQC, qc)
      case Phase.Commit =>
        put(Key.CommitQC, qc)
    }

  def setLastExecutedBlockHash(blockHash: A#Hash): KVStore[N, Unit] =
    put(Key.LastExecutedBlockHash, blockHash)

  def getBundle: KVStoreRead[N, ViewStateStorage.Bundle[A]] =
    (
      read(Key.ViewNumber),
      read(Key.PrepareQC),
      read(Key.LockedQC),
      read(Key.CommitQC),
      read(Key.LastExecutedBlockHash)
    ).mapN(ViewStateStorage.Bundle.apply[A] _)

}

object ViewStateStorage {

  /** Storing elements of the view state individually under separate keys,
    * because they get written independently.
    */
  trait Keys[A <: Agreement] {
    sealed trait Key[V]
    object Key {
      case object ViewNumber            extends Key[ViewNumber]
      case object PrepareQC             extends Key[QuorumCertificate[A]]
      case object LockedQC              extends Key[QuorumCertificate[A]]
      case object CommitQC              extends Key[QuorumCertificate[A]]
      case object LastExecutedBlockHash extends Key[A#Hash]

      implicit def encoder[V]: Encoder[Key[V]] =
        scodec.codecs.implicits.implicitStringCodec
          .contramap[Key[V]](_.toString)
    }
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
    implicit val kvn  = KVStore.instance[N]
    implicit val kvrn = KVStoreRead.instance[N]
    implicit val keys = new Keys[A] {}
    import keys.Key

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
