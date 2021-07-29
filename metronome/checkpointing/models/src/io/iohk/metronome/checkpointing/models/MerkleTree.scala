package io.iohk.metronome.checkpointing.models

import io.iohk.metronome.core.Tagger
import io.iohk.metronome.crypto.hash.Keccak256
import scodec.bits.ByteVector

import scala.annotation.tailrec

sealed trait MerkleTree {
  def hash: MerkleTree.Hash
}

object MerkleTree {
  object Hash extends Tagger[ByteVector]
  type Hash = Hash.Tagged

  /** MerkleTree with no elements */
  val empty = Leaf(Hash(Keccak256(ByteVector.empty)))

  private val hashFn: (Hash, Hash) => Hash =
    (a, b) => Hash(Keccak256(a ++ b))

  case class Node(hash: Hash, left: MerkleTree, right: Option[MerkleTree])
      extends MerkleTree
  case class Leaf(hash: Hash) extends MerkleTree

  /** Merkle proof that some leaf content is part of the tree.
    *
    * It is expected that the root hash and the leaf itself is available to
    * the verifier, so the proof only contains things the verifier doesn't
    * know, which is the overall size of the tree and the position of the leaf
    * among its siblings leaves. Based on that it is possible to use the sibling
    * hash path to check whether they add up to the root hash.
    *
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
    * Note that the length of binary representation of `leafIndex` corresponds
    * to the height of the tree, e.g. `0010` for a tree of height 4 (9 to 16 leaves).
    */
  case class Proof(
      // Position of the leaf in the lowest level.
      leafIndex: Int,
      // Hashes of the "other" side of the tree, level by level,
      // starting from the lowest up to the highest.
      siblingPath: IndexedSeq[Hash]
  )

  def build(elems: Iterable[Hash]): MerkleTree = {
    @tailrec
    def buildTree(nodes: Seq[MerkleTree]): MerkleTree = {
      if (nodes.size == 1)
        nodes.head
      else {
        val paired = nodes.grouped(2).toSeq.map {
          case Seq(a, b) =>
            Node(hashFn(a.hash, b.hash), a, Some(b))
          case Seq(a) =>
            // if the element has no pair we hash it with itself
            Node(hashFn(a.hash, a.hash), a, None)
        }
        buildTree(paired)
      }
    }

    if (elems.isEmpty)
      empty
    else
      buildTree(elems.toSeq.map(Leaf(_)))
  }

  def verifyProof(
      proof: Proof,
      root: Hash,
      leaf: Hash
  ): Boolean = {
    def verify(currentHash: Hash, height: Int, siblings: Seq[Hash]): Hash = {
      if (siblings.isEmpty)
        currentHash
      else {
        val goLeft = shouldTraverseLeft(height, proof.leafIndex)
        val nextHash =
          if (goLeft) hashFn(currentHash, siblings.head)
          else hashFn(siblings.head, currentHash)

        verify(nextHash, height + 1, siblings.tail)
      }
    }

    // This is required so for example on a single element tree a `Proof(4, Nil)` is not accepted, only `Proof(0, Nil)`,
    // otherwise it would make the `CheckpointCertificate` malleable by adding arbitrarily large leaf indexes,
    // as long as the LSB side that the verification uses is correct.
    val maxLeafIndex = (1 << proof.siblingPath.length) - 1

    proof.leafIndex <= maxLeafIndex &&
    verify(leaf, 1, proof.siblingPath) == root
  }

  def generateProofFromIndex(root: MerkleTree, index: Int): Option[Proof] = {
    if (index < 0 || index >= findSize(root))
      None
    else {
      val siblings = findSiblings(root, findHeight(root), index)
      Some(Proof(index, siblings))
    }
  }

  def generateProofFromHash(root: MerkleTree, elem: Hash): Option[Proof] = {
    if (root == empty)
      None
    else
      findElem(root, elem).map { index =>
        val siblings = findSiblings(root, findHeight(root), index)
        Proof(index, siblings)
      }
  }

  @tailrec
  /** Finds tree height based on leftmost branch traversal */
  private def findHeight(tree: MerkleTree, height: Int = 0): Int = tree match {
    case Leaf(_)          => height
    case Node(_, left, _) => findHeight(left, height + 1)
  }

  @tailrec
  /** Finds the tree size (number of leaves), by traversing the rightmost branch */
  private def findSize(tree: MerkleTree, maxIndex: Int = 0): Int = tree match {
    case `empty` =>
      0
    case Leaf(_) =>
      maxIndex + 1
    case Node(_, left, None) =>
      findSize(left, maxIndex << 1)
    case Node(_, _, Some(right)) =>
      findSize(right, maxIndex << 1 | 1)
  }

  /** Looks up an element hash in the tree returning its index if it exists */
  private def findElem(
      tree: MerkleTree,
      elem: Hash,
      index: Int = 0
  ): Option[Int] = tree match {
    case Leaf(`elem`) =>
      Some(index)
    case Leaf(_) =>
      None
    case Node(_, left, None) =>
      findElem(left, elem, index << 1)
    case Node(_, left, Some(right)) =>
      findElem(left, elem, index << 1) orElse
        findElem(right, elem, index << 1 | 1)
  }

  /** Traverses the tree from root towards the leaf collecting the hashes of siblings nodes.
    * If a node has only one child then that child's hash is collected. Theses hashes constitute
    * the Merkle proof, they are returned ordered from lowest to highest (with regard to
    * the height of the tree)
    */
  private def findSiblings(
      tree: MerkleTree,
      height: Int,
      leafIndex: Int
  ): IndexedSeq[Hash] = tree match {
    case Leaf(_) =>
      Vector.empty

    case Node(_, left, None) =>
      if (!shouldTraverseLeft(height, leafIndex))
        Vector.empty
      else
        findSiblings(left, height - 1, leafIndex) :+ left.hash

    case Node(_, left, Some(right)) =>
      val goLeft              = shouldTraverseLeft(height, leafIndex)
      val (traverse, sibling) = if (goLeft) (left, right) else (right, left)
      findSiblings(traverse, height - 1, leafIndex) :+ sibling.hash
  }

  /** Determines tree traversal direction from a given height towards the leaf indicated
    * by the index:
    *
    * true - traverse left child (take right hash)
    * false - traverse right
    */
  private def shouldTraverseLeft(height: Int, leafIndex: Int): Boolean =
    (leafIndex >> (height - 1) & 1) == 0
}
