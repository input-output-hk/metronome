package io.iohk.metronome.storage

import cats.implicits._
import scala.collection.immutable.Queue

/** Storage for nodes that maintains parent-child relationships as well,
  * to facilitate tree traversal and pruning.
  *
  * It is assumed that the application maintains some pointers into the tree
  * where it can start traversing from, e.g. the last Commit Quorum Certificate
  * would point at a block hash which would serve as the entry point.
  *
  * This component is currently tested through `BlockStorage`.
  */
class KVTree[N, K, V](
    nodeColl: KVCollection[N, K, V],
    nodeMetaColl: KVCollection[N, K, KVTree.NodeMeta[K]],
    parentToChildrenColl: KVCollection[N, K, Set[K]]
)(implicit ev: KVTree.Node[K, V]) {
  import KVTree.NodeMeta

  private implicit val kvn  = KVStore.instance[N]
  private implicit val kvrn = KVStoreRead.instance[N]

  /** Insert a node into the store, and if the parent still exists,
    * then add this node to its children.
    */
  def put(value: V): KVStore[N, Unit] = {
    val nodeKey = ev.key(value)
    val meta =
      NodeMeta(ev.parentKey(value), ev.height(value))

    nodeColl.put(nodeKey, value) >>
      nodeMetaColl.put(nodeKey, meta) >>
      parentToChildrenColl.alter(meta.parentKey) { maybeChildren =>
        maybeChildren orElse Set.empty.some map (_ + nodeKey)
      }

  }

  /** Retrieve a node by key, if it exists. */
  def get(key: K): KVStoreRead[N, Option[V]] =
    nodeColl.read(key)

  /** Check whether a node is present in the tree. */
  def contains(key: K): KVStoreRead[N, Boolean] =
    nodeMetaColl.read(key).map(_.isDefined)

  /** Check how many children the node has in the tree. */
  private def childCount(key: K): KVStoreRead[N, Int] =
    parentToChildrenColl.read(key).map(_.fold(0)(_.size))

  /** Check whether the parent of the node is present in the tree. */
  private def hasParent(key: K): KVStoreRead[N, Boolean] =
    nodeMetaColl.read(key).flatMap {
      case None       => KVStoreRead[N].pure(false)
      case Some(meta) => contains(meta.parentKey)
    }

  private def getParentKey(
      key: K
  ): KVStoreRead[N, Option[K]] =
    nodeMetaColl.read(key).map(_.map(_.parentKey))

  /** Check whether it's safe to delete a node.
    *
    * A node is safe to delete if doing so doesn't break up the tree
    * into a forest, in which case we may have nodes we cannot reach
    * by traversal, leaking space.
    *
    * This is true if the node has no children,
    * or it has no parent and at most one child.
    */
  private def canDelete(key: K): KVStoreRead[N, Boolean] =
    (hasParent(key), childCount(key)).mapN {
      case (_, 0)     => true
      case (false, 1) => true
      case _          => false
    }

  /** Delete a node by hash, if doing so wouldn't break the tree;
    * otherwise do nothing.
    *
    * Return `true` if node has been deleted, `false` if not.
    *
    * If this is not efficent enough, then move the deletion traversal
    * logic into the this class so it can make sure all the invariants
    * are maintained, e.g. collect all  hashes that can be safely deleted
    * and then do so without checks.
    */
  def delete(key: K): KVStore[N, Boolean] =
    canDelete(key).lift.flatMap { ok =>
      deleteUnsafe(key).whenA(ok).as(ok)
    }

  /** Delete a node and remove it from any parent-to-child mapping,
    * without any checking for the tree structure invariants.
    */
  def deleteUnsafe(key: K): KVStore[N, Unit] = {
    def deleteIfEmpty(maybeChildren: Option[Set[K]]) =
      maybeChildren.filter(_.nonEmpty)

    getParentKey(key).lift.flatMap {
      case None =>
        KVStore[N].unit
      case Some(parentKey) =>
        parentToChildrenColl.alter(parentKey) { maybeChildren =>
          deleteIfEmpty(maybeChildren.map(_ - key))
        }
    } >>
      nodeColl.delete(key) >>
      nodeMetaColl.delete(key) >>
      // Keep the association from existing children, until they last one is deleted.
      parentToChildrenColl.alter(key)(deleteIfEmpty)
  }

  /** Get the ancestor chain of a node from the root, including the node itself.
    *
    * If the node is not in the tree, the result will be empty,
    * otherwise `head` will be the root of the node tree,
    * and `last` will be the node itself.
    */
  def getPathFromRoot(key: K): KVStoreRead[N, List[K]] = {
    def loop(
        key: K,
        acc: List[K]
    ): KVStoreRead[N, List[K]] = {
      getParentKey(key).flatMap {
        case None =>
          // This node doesn't exist in the tree, so our ancestry is whatever we collected so far.
          KVStoreRead[N].pure(acc)

        case Some(parentKey) =>
          // So at least `key` exists in the tree.
          loop(parentKey, key :: acc)
      }
    }
    loop(key, Nil)
  }

  /** Get the ancestor chain between two hashes in the chain, if there is one.
    *
    * If either of the nodes are not in the tree, or there's no path between them,
    * return an empty list. This can happen if we have already pruned away the ancestry as well.
    */
  def getPathFromAncestor(
      ancestorKey: K,
      descendantKey: K
  ): KVStoreRead[N, List[K]] = {
    def loop(
        key: K,
        acc: List[K],
        maxDistance: Long
    ): KVStoreRead[N, List[K]] = {
      if (key == ancestorKey) {
        KVStoreRead[N].pure(key :: acc)
      } else if (maxDistance == 0) {
        KVStoreRead[N].pure(Nil)
      } else {
        nodeMetaColl.read(key).flatMap {
          case None =>
            KVStoreRead[N].pure(Nil)
          case Some(meta) =>
            loop(meta.parentKey, key :: acc, maxDistance - 1)
        }
      }
    }

    (
      nodeMetaColl.read(ancestorKey),
      nodeMetaColl.read(descendantKey)
    ).mapN((_, _))
      .flatMap {
        case (Some(ameta), Some(dmeta)) =>
          loop(
            descendantKey,
            Nil,
            maxDistance = dmeta.height - ameta.height
          )
        case _ => KVStoreRead[N].pure(Nil)
      }
  }

  /** Collect all descendants of a node, including the node itself.
    *
    * The result will start with the nodes furthest away,
    * so it should be safe to delete them in the same order;
    * `last` will be the node itself.
    *
    * The `skip` parameter can be used to avoid traversing
    * branches that we want to keep during deletion.
    */
  def getDescendants(
      key: K,
      skip: Set[K] = Set.empty
  ): KVStoreRead[N, List[K]] = {
    // BFS traversal.
    def loop(
        queue: Queue[K],
        acc: List[K]
    ): KVStoreRead[N, List[K]] = {
      queue.dequeueOption match {
        case None =>
          KVStoreRead[N].pure(acc)

        case Some((key, queue)) if skip(key) =>
          loop(queue, acc)

        case Some((key, queue)) =>
          parentToChildrenColl.read(key).flatMap {
            case None =>
              // Since we're not inserting an empty child set,
              // we can't tell here if the node exists or not.
              loop(queue, key :: acc)
            case Some(children) =>
              loop(queue ++ children, key :: acc)
          }
      }
    }

    loop(Queue(key), Nil).flatMap {
      case result @ List(`key`) =>
        result.filterA(contains)
      case result =>
        KVStoreRead[N].pure(result)
    }
  }

  /** Delete all nodes which are not descendants of a given node, making it the new root.
    *
    * Return the list of deleted node keys.
    */
  def pruneNonDescendants(key: K): KVStore[N, List[K]] =
    getPathFromRoot(key).lift.flatMap {
      case Nil =>
        KVStore[N].pure(Nil)

      case path @ (rootHash :: _) =>
        // The safe order to delete nodes would be to go down the main chain
        // from the root, delete each non-mainchain child, then the parent,
        // then descend on the main chain until we hit `key`.

        // A similar effect can be achieved by collecting all descendants
        // of the root, then deleting everything that isn't on the main chain,
        // from the children towards the root, and finally the main chain itself,
        // going from the root towards the children.
        val isMainChain = path.toSet

        for {
          deleteables <- getDescendants(rootHash, skip = Set(key)).lift
          _           <- deleteables.filterNot(isMainChain).traverse(deleteUnsafe(_))
          _           <- path.init.traverse(deleteUnsafe(_))
        } yield deleteables
    }

  /** Remove all nodes in a tree, given by a key that's in the tree,
    * except perhaps a new root (and its descendants) we want to keep.
    *
    * This is used to delete an old tree when starting a new that's most likely
    * not connected to it, and would otherwise result in a forest.
    */
  def purgeTree(
      key: K,
      keep: Option[K]
  ): KVStore[N, List[K]] =
    getPathFromRoot(key).lift.flatMap {
      case Nil =>
        KVStore[N].pure(Nil)

      case rootHash :: _ =>
        for {
          ds <- getDescendants(rootHash, skip = keep.toSet).lift
          // Going from the leaves towards the root.
          _ <- ds.reverse.traverse(deleteUnsafe(_))
        } yield ds
    }
}

object KVTree {

  /** Type class for the node-like values stored in the tree. */
  trait Node[K, V] {
    def key(value: V): K
    def parentKey(value: V): K
    def height(value: V): Long
  }

  /** Properties about the nodes that we frequently need for traversal. */
  case class NodeMeta[K](
      parentKey: K,
      height: Long
  )
  object NodeMeta {
    import scodec.Codec

    implicit def codec[K: Codec]: Codec[NodeMeta[K]] = {
      import scodec.codecs.implicits._
      Codec.deriveLabelledGeneric
    }
  }
}
