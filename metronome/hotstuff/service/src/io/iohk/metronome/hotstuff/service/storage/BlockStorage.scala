package io.iohk.metronome.hotstuff.service.storage

import cats.implicits._
import io.iohk.metronome.storage.{KVStore, KVCollection}
import io.iohk.metronome.hotstuff.consensus.basic.{Agreement, Block}
import scala.collection.immutable.Queue

/** Storage for blocks that maintains parent-child relationships as well,
  * to facilitate tree traversal and pruning.
  *
  * It is assumed that the application maintains some pointers into the tree
  * where it can start traversing from, e.g. the last Commit Quorum Certificate
  * would point at a block hash which would serve as the entry point.
  */
class BlockStorage[N, A <: Agreement: Block](
    blockColl: KVCollection[N, A#Hash, A#Block],
    childToParentColl: KVCollection[N, A#Hash, A#Hash],
    parentToChildrenColl: KVCollection[N, A#Hash, Set[A#Hash]]
) {
  private implicit val kvn = KVStore.instance[N]

  /** Insert a block into the store, and if the parent still exists,
    * then add this block to its children.
    */
  def put(block: A#Block): KVStore[N, Unit] = {
    val blockHash  = implicitly[Block[A]].blockHash(block)
    val parentHash = implicitly[Block[A]].parentBlockHash(block)

    blockColl.put(blockHash, block) >>
      childToParentColl.put(blockHash, parentHash) >>
      parentToChildrenColl.update(parentHash, _ + blockHash)
  }

  /** Retrieve a block by hash, if it exists. */
  def get(blockHash: A#Hash): KVStore[N, Option[A#Block]] =
    blockColl.get(blockHash)

  /** Check whether a block is present in the tree. */
  def contains(blockHash: A#Hash): KVStore[N, Boolean] =
    childToParentColl.get(blockHash).map(_.isDefined)

  /** Check how many children the block has in the tree. */
  private def childCount(blockHash: A#Hash): KVStore[N, Int] =
    parentToChildrenColl.get(blockHash).map(_.fold(0)(_.size))

  /** Check whether the parent of the block is present in the tree. */
  private def hasParent(blockHash: A#Hash): KVStore[N, Boolean] =
    childToParentColl.get(blockHash).flatMap {
      case None             => KVStore[N].pure(false)
      case Some(parentHash) => contains(parentHash)
    }

  /** Check whether it's safe to delete a block.
    *
    * A block is safe to delete if doing so doesn't break up the tree
    * into a forest, in which case we may have blocks we cannot reach
    * by traversal, leaking space.
    *
    * This is true if the block has no children,
    * or it has no parent and at most one child.
    */
  private def canDelete(blockHash: A#Hash): KVStore[N, Boolean] =
    (hasParent(blockHash), childCount(blockHash)).mapN {
      case (_, 0)     => true
      case (false, 1) => true
      case _          => false
    }

  /** Delete a block by hash, if doing so wouldn't break the tree;
    * otherwise do nothing.
    *
    * Return `true` if block has been deleted, `false` if not.
    *
    * If this is not efficent enough, then move the deletion traversal
    * logic into the this class so it can make sure all the invariants
    * are maintained, e.g. collect all  hashes that can be safely deleted
    * and then do so without checks.
    */
  def delete(blockHash: A#Hash): KVStore[N, Boolean] =
    canDelete(blockHash).flatMap { ok =>
      deleteUnsafe(blockHash).whenA(ok).as(ok)
    }

  /** Delete a block and remove it from any parent-to-child mapping,
    * without any checking for the tree structure invariants.
    */
  private def deleteUnsafe(blockHash: A#Hash): KVStore[N, Unit] =
    blockColl.delete(blockHash) >>
      childToParentColl.get(blockHash).flatMap {
        case None =>
          KVStore[N].unit
        case Some(parentHash) =>
          childToParentColl.delete(blockHash) >>
            parentToChildrenColl.update(parentHash, _ - blockHash)
      }

  /** Get the ancestor chain of a block from the root,
    * including the block itself.
    *
    * If the block is not in the tree, the result will be empty,
    * otherwise `head` will be the root of the block tree,
    * and `last` will be the block itself.
    */
  def getPathFromRoot(blockHash: A#Hash): KVStore[N, List[A#Hash]] = {
    def loop(
        blockHash: A#Hash,
        acc: List[A#Hash]
    ): KVStore[N, List[A#Hash]] = {
      childToParentColl.get(blockHash).flatMap {
        case None =>
          // This block doesn't exist in the tree, so our ancestry is whatever we collected so far.
          KVStore[N].pure(acc)

        case Some(parentHash) =>
          // So at least `blockHash` exists in the tree.
          loop(parentHash, blockHash :: acc)
      }
    }
    loop(blockHash, Nil)
  }

  /** Get the ancestor chain of a block up to the root,
    * excluding the block itself.
    *
    * If the block is not in the tree, the result will be empty,
    * otherwise `head` will be the parent of the block,
    * and `last` will be the root of block tree.
    */
  def getAncestors(blockHash: A#Hash): KVStore[N, List[A#Hash]] =
    getPathFromRoot(blockHash) map {
      case Nil  => Nil
      case path => path.reverse.tail
    }

  /** Collect all descendants of a block.
    *
    * The result will start with the blocks furthest away,
    * so it should be safe to delete them in the same order.
    *
    * The `skip` parameter can be used to avoid traversing
    * branches that we want to keep during deletion.
    */
  def getDescendants(
      blockHash: A#Hash,
      skip: Set[A#Hash] = Set.empty
  ): KVStore[N, List[A#Hash]] = {
    // BFS traversal.
    def loop(
        queue: Queue[A#Hash],
        acc: List[A#Hash]
    ): KVStore[N, List[A#Hash]] = {
      queue.dequeueOption match {
        case None =>
          KVStore[N].pure(acc)

        case Some((blockHash, queue)) if skip(blockHash) =>
          loop(queue, acc)

        case Some((blockHash, queue)) =>
          parentToChildrenColl.get(blockHash).flatMap {
            case None =>
              loop(queue, blockHash :: acc)
            case Some(children) =>
              loop(queue ++ children, blockHash :: acc)
          }
      }
    }
    loop(Queue(blockHash), Nil)
  }

  /** Delete all blocks which are not descendants of a given block,
    * making it the new root.
    *
    * Return the list of deleted block hashes.
    */
  def pruneNonDescendants(blockHash: A#Hash): KVStore[N, List[A#Hash]] =
    getPathFromRoot(blockHash).flatMap {
      case Nil =>
        KVStore[N].pure(Nil)

      case path @ (rootHash :: _) =>
        // The safe order to delete blocks would be to go down the main chain
        // from the root, delete each non-mainchain child, then the parent,
        // then descend on the main chain until we hit `blockHash`.

        // A similar effect can be achieved by collecting all descendants
        // of the root, then deleting everything that isn't on the main chain,
        // from the children towards the root, and finally the main chain itself,
        // going from the root towards the children.
        val isMainChain = path.toSet

        for {
          deleteables <- getDescendants(rootHash, skip = Set(blockHash))
          _           <- deleteables.filterNot(isMainChain).traverse(deleteUnsafe(_))
          _           <- path.init.traverse(deleteUnsafe(_))
        } yield deleteables
    }
}
