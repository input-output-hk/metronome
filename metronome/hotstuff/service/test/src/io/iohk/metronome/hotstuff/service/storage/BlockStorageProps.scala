package io.iohk.metronome.hotstuff.service.storage

import cats.implicits._
import io.iohk.metronome.storage.{KVCollection, KVStoreState}
import io.iohk.metronome.hotstuff.consensus.basic.{Agreement, Block => BlockOps}
import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.{all, forAll, propBoolean}
import scodec.codecs.implicits._
import scodec.Codec
import scala.util.Random

object BlockStorageProps extends Properties("BlockStorage") {

  case class TestBlock(id: String, parentId: String, height: Long) {
    def isGenesis = parentId.isEmpty
  }

  object TestAgreement extends Agreement {
    type Block = TestBlock
    type Hash  = String
    type PSig  = Nothing
    type GSig  = Unit
    type PKey  = Int
    type SKey  = Nothing

    implicit val block = new BlockOps[TestAgreement] {
      override def blockHash(b: TestBlock)       = b.id
      override def parentBlockHash(b: TestBlock) = b.parentId
      override def height(b: Block): Long        = b.height
      override def isValid(b: Block)             = true
    }
  }
  type TestAgreement = TestAgreement.type
  type Hash          = TestAgreement.Hash

  implicit def `Codec[Set[T]]`[T: Codec] =
    implicitly[Codec[List[T]]].xmap[Set[T]](_.toSet, _.toList)

  type Namespace = String
  object Namespace {
    val Blocks          = "blocks"
    val BlockMetas      = "block-metas"
    val BlockToChildren = "block-to-children"
  }

  object TestBlockStorage
      extends BlockStorage[Namespace, TestAgreement](
        new KVCollection[Namespace, Hash, TestBlock](Namespace.Blocks),
        new KVCollection[Namespace, Hash, BlockStorage.BlockMeta[
          TestAgreement
        ]](Namespace.BlockMetas),
        new KVCollection[Namespace, Hash, Set[Hash]](Namespace.BlockToChildren)
      )

  object TestKVStore extends KVStoreState[Namespace] {
    def build(tree: List[TestBlock]): Store = {
      val insert = tree.map(TestBlockStorage.put).sequence
      compile(insert).runS(Map.empty).value
    }
  }

  implicit class TestStoreOps(store: TestKVStore.Store) {
    def putBlock(block: TestBlock) =
      TestKVStore.compile(TestBlockStorage.put(block)).runS(store).value

    def containsBlock(blockHash: Hash) =
      TestKVStore
        .compile(TestBlockStorage.contains(blockHash))
        .run(store)

    def getBlock(blockHash: Hash) =
      TestKVStore
        .compile(TestBlockStorage.get(blockHash))
        .run(store)

    def deleteBlock(blockHash: Hash) =
      TestKVStore
        .compile(TestBlockStorage.delete(blockHash))
        .run(store)
        .value

    def getPathFromRoot(blockHash: Hash) =
      TestKVStore
        .compile(TestBlockStorage.getPathFromRoot(blockHash))
        .run(store)

    def getPathFromAncestor(
        ancestorBlockHash: Hash,
        descendantBlockHash: Hash
    ) =
      TestKVStore
        .compile(
          TestBlockStorage
            .getPathFromAncestor(
              ancestorBlockHash,
              descendantBlockHash
            )
        )
        .run(store)

    def getDescendants(blockHash: Hash) =
      TestKVStore
        .compile(TestBlockStorage.getDescendants(blockHash))
        .run(store)

    def pruneNonDescendants(blockHash: Hash) =
      TestKVStore
        .compile(TestBlockStorage.pruneNonDescendants(blockHash))
        .run(store)
        .value
  }

  def genBlockId: Gen[Hash] =
    Gen.uuid.map(_.toString)

  /** Generate a block with a given parent, using the next available ID. */
  def genBlock(parent: TestBlock): Gen[TestBlock] =
    genBlockId.map { uuid =>
      TestBlock(uuid, parentId = parent.id, height = parent.height + 1)
    }

  def genBlock: Gen[TestBlock] =
    for {
      id       <- genBlockId
      parentId <- genBlockId
      height   <- Gen.posNum[Long]
    } yield TestBlock(id, parentId, height)

  // A block we can pass as parent to tree generators so the first block is a
  // genesis block with height = 0 and parentId = "".
  val preGenesisParent = TestBlock(id = "", parentId = "", height = -1)

  /** Generate a (possibly empty) block tree. */
  def genBlockTree(parent: TestBlock): Gen[List[TestBlock]] =
    for {
      childCount <- Gen.frequency(
        3 -> 0,
        5 -> 1,
        2 -> 2
      )
      children <- Gen.listOfN(
        childCount, {
          for {
            block <- genBlock(parent)
            tree  <- genBlockTree(block)
          } yield block +: tree
        }
      )
    } yield children.flatten

  def genBlockTree: Gen[List[TestBlock]] =
    genBlockTree(preGenesisParent)

  def genNonEmptyBlockTree(parent: TestBlock): Gen[List[TestBlock]] = for {
    child <- genBlock(parent)
    tree  <- genBlockTree(child)
  } yield child +: tree

  def genNonEmptyBlockTree: Gen[List[TestBlock]] =
    genNonEmptyBlockTree(preGenesisParent)

  case class TestData(
      tree: List[TestBlock],
      store: TestKVStore.Store
  )
  object TestData {
    def apply(tree: List[TestBlock]): TestData = {
      val store = TestKVStore.build(tree)
      TestData(tree, store)
    }
  }

  def genExisting = for {
    tree     <- genNonEmptyBlockTree
    existing <- Gen.oneOf(tree)
    data = TestData(tree)
  } yield (data, existing)

  def genNonExisting = for {
    tree        <- genBlockTree
    nonExisting <- genBlock
    data = TestData(tree)
  } yield (data, nonExisting)

  def genSubTree = for {
    tree <- genNonEmptyBlockTree
    leaf = tree.last
    subTree <- genBlockTree(parent = leaf)
    data = TestData(tree ++ subTree)
  } yield (data, leaf, subTree)

  property("put") = forAll(genNonExisting) { case (data, block) =>
    val s = data.store.putBlock(block)
    s(Namespace.Blocks)(block.id) == block
    s(Namespace.BlockMetas)(block.id)
      .asInstanceOf[BlockStorage.BlockMeta[TestAgreement]]
      .parentBlockHash == block.parentId
  }

  property("put unordered") = forAll {
    for {
      ordered <- genNonEmptyBlockTree
      seed    <- arbitrary[Int]
      unordered = new Random(seed).shuffle(ordered)
    } yield (ordered, unordered)
  } { case (ordered, unordered) =>
    val orderedStore   = TestKVStore.build(ordered)
    val unorderedStore = TestKVStore.build(unordered)
    orderedStore == unorderedStore
  }

  property("contains existing") = forAll(genExisting) { case (data, existing) =>
    data.store.containsBlock(existing.id)
  }

  property("contains non-existing") = forAll(genNonExisting) {
    case (data, nonExisting) =>
      !data.store.containsBlock(nonExisting.id)
  }

  property("get existing") = forAll(genExisting) { case (data, existing) =>
    data.store.getBlock(existing.id).contains(existing)
  }

  property("get non-existing") = forAll(genNonExisting) {
    case (data, nonExisting) =>
      data.store.getBlock(nonExisting.id).isEmpty
  }

  property("delete existing") = forAll(genExisting) { case (data, existing) =>
    val childCount = data.tree.count(_.parentId == existing.id)
    val noParent   = !data.tree.exists(_.id == existing.parentId)
    val (s, ok)    = data.store.deleteBlock(existing.id)
    all(
      "deleted" |: s.containsBlock(existing.id) == !ok,
      "ok" |: ok && (childCount == 0 || childCount == 1 && noParent) || !ok
    )
  }

  property("delete non-existing") = forAll(genNonExisting) {
    case (data, nonExisting) =>
      data.store.deleteBlock(nonExisting.id)._2 == true
  }

  property("reinsert one") = forAll(genExisting) { case (data, existing) =>
    val (deleted, _) = data.store.deleteBlock(existing.id)
    val inserted     = deleted.putBlock(existing)
    // The existing child relationships should not be lost.
    inserted == data.store
  }

  property("getPathFromRoot existing") = forAll(genExisting) {
    case (data, existing) =>
      val path = data.store.getPathFromRoot(existing.id)
      all(
        "nonEmpty" |: path.nonEmpty,
        "head" |: path.headOption.contains(data.tree.head.id),
        "last" |: path.lastOption.contains(existing.id)
      )
  }

  property("getPathFromRoot non-existing") = forAll(genNonExisting) {
    case (data, nonExisting) =>
      data.store.getPathFromRoot(nonExisting.id).isEmpty
  }

  property("getPathFromAncestor") = forAll(
    for {
      prefix <- genNonEmptyBlockTree
      ancestor = prefix.last
      postfix    <- genNonEmptyBlockTree(ancestor)
      descendant <- Gen.oneOf(postfix)
      data = TestData(prefix ++ postfix)
      nonExisting <- genBlock
    } yield (data, ancestor, descendant, nonExisting)
  ) { case (data, ancestor, descendant, nonExisting) =>
    def getPath(a: TestBlock, d: TestBlock) =
      data.store.getPathFromAncestor(a.id, d.id)

    def pathExists(a: TestBlock, d: TestBlock) = {
      val path = getPath(a, d)
      path.nonEmpty &&
      path.distinct.size == path.size &&
      path.head == a.id &&
      path.last == d.id &&
      (path.init zip path.tail).forall { case (parentHash, childHash) =>
        data.store.getBlock(childHash).get.parentId == parentHash
      }
    }

    def pathNotExists(a: TestBlock, d: TestBlock) =
      getPath(a, d).isEmpty

    all(
      "fromAtoD" |: pathExists(ancestor, descendant),
      "fromDtoA" |: pathNotExists(descendant, ancestor),
      "fromAtoA" |: pathExists(ancestor, ancestor),
      "fromDtoD" |: pathExists(descendant, descendant),
      "fromAtoN" |: pathNotExists(ancestor, nonExisting),
      "fromNtoD" |: pathNotExists(nonExisting, descendant)
    )
  }

  property("getDescendants existing") = forAll(genSubTree) {
    case (data, block, subTree) =>
      val ds  = data.store.getDescendants(block.id)
      val dss = ds.toSet
      all(
        "nonEmpty" |: ds.nonEmpty,
        "last" |: ds.lastOption.contains(block.id),
        "size" |: ds.size == subTree.size + 1,
        "subtree" |: subTree.forall(block => dss.contains(block.id))
      )
  }

  property("getDescendants non-existing") = forAll(genNonExisting) {
    case (data, nonExisting) =>
      data.store.getDescendants(nonExisting.id).isEmpty
  }

  property("getDescendants delete") = forAll(genSubTree) {
    case (data, block, _) =>
      val ds = data.store.getDescendants(block.id)

      val (deleted, ok) = ds.foldLeft((data.store, true)) {
        case ((store, oks), blockHash) =>
          val (deleted, ok) = store.deleteBlock(blockHash)
          (deleted, oks && ok)
      }

      val prefixTree  = data.tree.takeWhile(_ != block)
      val prefixStore = TestKVStore.build(prefixTree)

      all(
        "ok" |: ok,
        "not contains deleted" |:
          ds.forall(!deleted.containsBlock(_)),
        "contains non deleted" |:
          prefixTree.map(_.id).forall(deleted.containsBlock(_)),
        "same as a rebuild" |:
          prefixStore == deleted
      )
  }

  property("pruneNonDescendants existing") = forAll(genSubTree) {
    case (data, block, subTree) =>
      val (s, ps)     = data.store.pruneNonDescendants(block.id)
      val pss         = ps.toSet
      val descendants = subTree.map(_.id).toSet
      val nonDescendants =
        data.tree.map(_.id).filterNot(descendants).filterNot(_ == block.id)
      all(
        "size" |: ps.size == nonDescendants.size,
        "pruned" |: nonDescendants.forall(pss),
        "deleted" |: nonDescendants.forall(!s.containsBlock(_)),
        "kept-block" |: s.containsBlock(block.id),
        "kept-descendants" |: descendants.forall(s.containsBlock(_))
      )
  }

  property("pruneNonDescendants non-existing") = forAll(genNonExisting) {
    case (data, nonExisting) =>
      data.store.pruneNonDescendants(nonExisting.id)._2.isEmpty
  }
}
