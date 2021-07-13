package io.iohk.metronome.hotstuff.service.storage

import io.iohk.metronome.storage.{KVCollection, KVTree}
import io.iohk.metronome.hotstuff.consensus.basic.{Agreement, Block}

/** Storage for blocks that maintains parent-child relationships as well,
  * to facilitate tree traversal and pruning.
  *
  * It is assumed that the application maintains some pointers into the tree
  * where it can start traversing from, e.g. the last Commit Quorum Certificate
  * would point at a block hash which would serve as the entry point.
  */
class BlockStorage[N, A <: Agreement: Block](
    blockColl: KVCollection[N, A#Hash, A#Block],
    blockMetaColl: KVCollection[N, A#Hash, KVTree.NodeMeta[A#Hash]],
    parentToChildrenColl: KVCollection[N, A#Hash, Set[A#Hash]]
) extends KVTree[N, A#Hash, A#Block](
      blockColl,
      blockMetaColl,
      parentToChildrenColl
    )(BlockStorage.node[A])

object BlockStorage {
  implicit def node[A <: Agreement: Block]: KVTree.Node[A#Hash, A#Block] =
    new KVTree.Node[A#Hash, A#Block] {
      override def key(value: A#Block): A#Hash =
        Block[A].blockHash(value)
      override def parentKey(value: A#Block): A#Hash =
        Block[A].parentBlockHash(value)
      override def height(value: A#Block): Long =
        Block[A].height(value)
    }
}
