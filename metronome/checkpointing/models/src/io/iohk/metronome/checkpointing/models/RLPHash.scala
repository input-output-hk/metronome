package io.iohk.metronome.checkpointing.models

import io.iohk.ethereum.rlp
import io.iohk.ethereum.rlp.RLPEncoder
import io.iohk.metronome.crypto
import io.iohk.metronome.crypto.hash.Keccak256
import io.iohk.metronome.core.Tagger
import scodec.bits.ByteVector
import scala.language.implicitConversions

/** Type class to produce a specific type of hash based on the RLP
  * representation of a type, where the hash type is typically
  * defined in the companion object of the type.
  */
trait RLPHasher[T] {
  type Hash
  def hash(value: T): Hash
}
object RLPHasher {
  type Aux[T, H] = RLPHasher[T] {
    type Hash = H
  }
}

/** Base class for types that have a hash value based on their RLP representation. */
abstract class RLPHash[T, H](implicit ev: RLPHasher.Aux[T, H]) { self: T =>
  lazy val hash: H = ev.hash(self)
}

/** Base class for companion objects for types that need hashes based on RLP.
  *
  * Every companion will define a separate `Hash` type, so we don't mix them up.
  */
abstract class RLPHashCompanion[T: RLPEncoder] extends RLPHasher[T] { self =>
  object Hash extends Tagger[ByteVector]
  override type Hash = Hash.Tagged

  override def hash(value: T): Hash =
    Hash(Keccak256(rlp.encode(value)))

  implicit val hasher: RLPHasher.Aux[T, Hash] = this

  implicit def `Hash => crypto.Hash`(h: Hash): crypto.hash.Hash =
    crypto.hash.Hash(h)
}
