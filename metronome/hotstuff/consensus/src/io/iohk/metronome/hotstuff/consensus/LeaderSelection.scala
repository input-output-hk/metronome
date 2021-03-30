package io.iohk.metronome.hotstuff.consensus

import io.iohk.metronome.crypto.hash.Keccak256
import scodec.bits.ByteVector

/** Strategy to pick the leader for a given view number from
  * federation of with a fixed size.
  */
trait LeaderSelection {

  /** Return the index of the federation member who should lead the view. */
  def leaderOf(viewNumber: ViewNumber, size: Int): Int
}

object LeaderSelection {

  /** Simple strategy cycling through leaders in a static order. */
  object RoundRobin extends LeaderSelection {
    override def leaderOf(viewNumber: ViewNumber, size: Int): Int =
      (viewNumber % size).toInt
  }

  /** Leader assignment based on view-number has not been discussed in the Hotstuff
    * paper and in general, it does not affect the safety and liveness.
    * However, it does affect worst-case latency.
    *
    * Consider a static adversary under a round-robin leader change scheme.
    * All the f nodes can set their public keys so that they are consecutive.
    * In such a scenario those f consecutive leaders can create timeouts leading
    * to an O(f) confirmation latency. (Recall that in a normal case, the latency is O(1)).
    *
    * A minor improvement to this is to assign leaders based on
    * "publicKeys((H256(viewNumber).toInt % size).toInt)".
    *
    * This leader order randomization via a hash function will ensure that even
    * if adversarial public keys are consecutive in PublicKey set, they are not
    * necessarily consecutive in leader order.
    *
    * Note that the above policy will not ensure that adversarial leaders are never consecutive,
    * but the probability of such occurrence will be lower under a static adversary.
    */
  object Hashing extends LeaderSelection {
    override def leaderOf(viewNumber: ViewNumber, size: Int): Int = {
      val bytes = ByteVector.fromLong(viewNumber) // big-endian
      val hash  = Keccak256(bytes)
      // If we prepend 0.toByte then it would treat it as unsigned, at the cost of an array copy.
      // Instead of doing that I'll just make sure we deal with negative modulo.
      val num = BigInt(hash.toArray)
      val mod = (num % size).toInt
      if (mod < 0) mod + size else mod
    }
  }
}
