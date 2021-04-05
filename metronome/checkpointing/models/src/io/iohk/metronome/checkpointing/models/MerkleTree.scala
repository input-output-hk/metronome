package io.iohk.metronome.checkpointing.models

import io.iohk.metronome.core.Tagger
import scodec.bits.ByteVector

object MerkleTree {
  object Hash extends Tagger[ByteVector] {
    val empty = apply(ByteVector.empty)
  }
  type Hash = Hash.Tagged

  /** Merkle proof that some leaf content is part of the tree.
    *
    * It is expected that the root hash and the leaf itself is available to
    * the verifier, so the proof only contains things the verifier doesn't
    * know, which is the overall size of the tree and the position of the leaf
    * among its siblings leaves. Based on that it is possible to use the sibling
    * hash path to check whether they add up to the root hash.
    *
    * `leafCount` gives the height of the binary tree: `leafCount = 2^h`
    * `leafIndex` can be interpreted as a binary number, which represents
    * the path from the root of the tree down to the leaf, with the bits
    * indicating whether to go left or right in each fork, while descending
    * the levels.
    *
    * For example, take the following Merkle tree:
    * ```
    *       h0123
    *     /       \
    *   h01       h23
    *  /   \     /   \
    * h0    h1  h2    h3
    * ```
    *
    * Say we want to prove that leaf 2 is part of the tree. The binary
    * representation of 2 is `10`, which, we can interpret as: go right,
    * then go left.
    *
    * The sibling path in the proof would be: `Vector(h3, h01)`.
    *
    * Based on this we can take the leaf value we know, reconstruct the hashes
    * from the bottom to the top, and compare it agains the root hash we know:
    *
    * ```
    * h2    = h(value)
    * h23   = h(h2, path(0))
    * h0123 = h(path(1), h23)
    * assert(h0123 == root)
    * ```
    *
    * The right/left decisions we gleaned from the `leafIndex` tell us the order
    * we have to pass the arguments to the hash function.
    *
    * Note that if `leafCount` would be higher, the binary representation of 2
    * would conceptually be longer, e.g. `0010` for a tree with 16 leaves.
    */
  case class Proof(
      // Position of the leaf in the lowest level.
      leafIndex: Int,
      // Number of leaves in the lowest level.
      leafCount: Int,
      // Hashes of the "other" side of the tree, level by level,
      // starting from the lowest up to the highest.
      siblingPath: IndexedSeq[MerkleTree.Hash]
  )
}
