package io.iohk.metronome.checkpointing.service.models

import io.iohk.ethereum.rlp
import io.iohk.ethereum.rlp.RLPEncoder
import io.iohk.metronome.crypto.hash.{Hash, Keccak256}

/** Calculate a hash based on the RLP representation of a value. */
object RLPHash {
  def apply[T: RLPEncoder](value: T): Hash =
    Keccak256(rlp.encode(value))
}

/** Mixin for classes that need hashing based on their RLP representation. */
trait RLPHash[T] { self: T =>
  protected implicit def encoder: RLPEncoder[T]

  lazy val hash = RLPHash[T](self)
}
