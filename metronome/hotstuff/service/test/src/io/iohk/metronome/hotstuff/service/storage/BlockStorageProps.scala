package io.iohk.metronome.hotstuff.service.storage

import cats.implicits._
import io.iohk.metronome.storage.{KVCollection, KVStoreState}
import io.iohk.metronome.hotstuff.consensus.basic.{Agreement, Block => BlockOps}
import java.util.UUID
import org.scalacheck._
import org.scalacheck.Prop.{all, forAll, propBoolean}
import scodec.codecs.implicits._
import scodec.Codec

object BlockStorageProps extends Properties("BlockStorage") {

  case class TestBlock(id: String, parentId: String) {
    def isGenesis = parentId.isEmpty
  }

  object TestAggreement extends Agreement {
    type Block = TestBlock
    type Hash  = String
    type PSig  = Nothing
    type GSig  = Nothing
    type PKey  = Nothing
    type SKey  = Nothing

    implicit val block = new BlockOps[TestAggreement] {
      override def blockHash(b: TestBlock)       = b.id
      override def parentBlockHash(b: TestBlock) = b.parentId
    }
  }
  type TestAggreement = TestAggreement.type
  type Hash           = TestAggreement.Hash

  implicit def `Codec[Set[T]]`[T: Codec] =
    implicitly[Codec[List[T]]].xmap[Set[T]](_.toSet, _.toList)

  type Namespace = String
  object Namespace {
    val Blocks          = "blocks"
    val BlockToParent   = "block-to-parent"
    val BlockToChildren = "block-to-children"
  }

  object TestBlockStorage
      extends BlockStorage[Namespace, TestAggreement](
        new KVCollection[Namespace, Hash, TestBlock](Namespace.Blocks),
        new KVCollection[Namespace, Hash, Hash](Namespace.BlockToParent),
        new KVCollection[Namespace, Hash, Set[Hash]](Namespace.BlockToChildren)
      )

  object TestKVStore extends KVStoreState[Namespace]

  implicit class TestStoreOps(store: TestKVStore.Store) {
    def putBlock(block: TestBlock) =
      TestKVStore.compile(TestBlockStorage.put(block)).runS(store).value

    def containsBlock(blockHash: Hash) =
      TestKVStore
        .compile(TestBlockStorage.contains(blockHash))
        .runA(store)
        .value

    def getBlock(blockHash: Hash) =
      TestKVStore
        .compile(TestBlockStorage.get(blockHash))
        .runA(store)
        .value

    def deleteBlock(blockHash: Hash) =
      TestKVStore
        .compile(TestBlockStorage.delete(blockHash))
        .run(store)
        .value

    def getPathFromRoot(blockHash: Hash) =
      TestKVStore
        .compile(TestBlockStorage.getPathFromRoot(blockHash))
        .runA(store)
        .value

    def getDescendants(blockHash: Hash) =
      TestKVStore
        .compile(TestBlockStorage.getDescendants(blockHash))
        .runA(store)
        .value

    def pruneNonDescendants(blockHash: Hash) =
      TestKVStore
        .compile(TestBlockStorage.pruneNonDescendants(blockHash))
        .run(store)
        .value
  }

  def genBlockId: Gen[Hash] =
    Gen.delay(UUID.randomUUID().toString)

  /** Generate a block with a given parent, using the next available ID. */
  def genBlock(parentId: Hash): Gen[TestBlock] =
    genBlockId.map { uuid =>
      TestBlock(uuid, parentId)
    }

  def genBlock: Gen[TestBlock] =
    genBlockId.flatMap(genBlock)

  /** Generate a (possibly empty) block tree. */
  def genBlockTree(parentId: Hash): Gen[List[TestBlock]] =
    for {
      childCount <- Gen.frequency(
        3 -> 0,
        5 -> 1,
        2 -> 2
      )
      children <- Gen.listOfN(
        childCount, {
          for {
            block <- genBlock(parentId)
            tree  <- genBlockTree(block.id)
          } yield block +: tree
        }
      )
    } yield children.flatten

  def genBlockTree: Gen[List[TestBlock]] =
    genBlockTree(parentId = "")

  def genNonEmptyBlockTree: Gen[List[TestBlock]] = for {
    genesis <- genBlock(parentId = "")
    tree    <- genBlockTree(genesis.id)
  } yield genesis +: tree

  case class TestData(
      tree: List[TestBlock],
      store: TestKVStore.Store
  )
  object TestData {
    def apply(tree: List[TestBlock]): TestData = {
      val insert = tree.map(TestBlockStorage.put).sequence
      val store  = TestKVStore.compile(insert).runS(Map.empty).value
      TestData(tree, store)
    }
  }

  def genTestData = for {
    tree        <- genNonEmptyBlockTree
    existing    <- Gen.oneOf(tree)
    nonExisting <- genBlock
    data = TestData(tree)
  } yield (data, existing, nonExisting)

  property("put") = forAll(genBlockTree.map(TestData(_)), genBlock) {
    case (data, block) =>
      val s = data.store.putBlock(block)
      s(Namespace.Blocks)(block.id) == block
      s(Namespace.BlockToParent)(block.id) == block.parentId
  }

  property("contains existing") = forAll(genTestData) {
    case (data, existing, _) =>
      data.store.containsBlock(existing.id)
  }

  property("contains non-existing") = forAll(genTestData) {
    case (data, _, nonExisting) =>
      !data.store.containsBlock(nonExisting.id)
  }

  property("get existing") = forAll(genTestData) { case (data, existing, _) =>
    data.store.getBlock(existing.id).contains(existing)
  }

  property("get non-existing") = forAll(genTestData) {
    case (data, _, nonExisting) =>
      data.store.getBlock(nonExisting.id).isEmpty
  }

  property("delete existing") = forAll(genTestData) {
    case (data, existing, _) =>
      val childCount = data.tree.count(_.parentId == existing.id)
      val noParent   = !data.tree.exists(_.id == existing.parentId)
      val (s, ok)    = data.store.deleteBlock(existing.id)
      all(
        "deleted" |: s.containsBlock(existing.id) == !ok,
        "ok" |: ok && (childCount == 0 || childCount == 1 && noParent) || !ok
      )
  }

  property("delete non-existing") = forAll(genTestData) {
    case (data, _, nonExisting) =>
      data.store.deleteBlock(nonExisting.id)._2 == true
  }

  property("getPathFromRoot existing") = forAll(genTestData) {
    case (data, existing, nonExisting) =>
      val path = data.store.getPathFromRoot(existing.id)
      all(
        "nonEmpty" |: path.nonEmpty,
        "head" |: path.headOption.contains(data.tree.head.id),
        "last" |: path.lastOption.contains(existing.id)
      )
  }

  property("getPathFromRoot non-existing") = forAll(genTestData) {
    case (data, _, nonExisting) =>
      data.store.getPathFromRoot(nonExisting.id).isEmpty
  }

  property("getDescendants existing") = forAll(genTestData) {
    case (data, existing, _) =>
      val ds = data.store.getDescendants(existing.id)
      all(
        "nonEmpty" |: ds.nonEmpty,
        "last" |: ds.lastOption.contains(existing.id),
        "path-from-root" |: ds.forall { d =>
          data.store.getPathFromRoot(d).contains(existing.id)
        },
        "head-is-leaf" |: ds.nonEmpty &&
          !data.tree.exists(_.parentId == ds.head)
      )
  }

  property("getDescendants non-existing") = forAll(genTestData) {
    case (data, _, nonExisting) =>
      data.store.getDescendants(nonExisting.id).isEmpty
  }

  property("pruneNonDescendants existing") = forAll(genTestData) {
    case (data, existing, _) =>
      val (s, ps)        = data.store.pruneNonDescendants(existing.id)
      val pruned         = ps.toSet
      val descendants    = data.store.getDescendants(existing.id).toSet
      val nonDescendants = data.tree.map(_.id).filterNot(descendants)
      nonDescendants.forall { x =>
        pruned(x) &&
        !s.containsBlock(x) &&
        !data.store.getPathFromRoot(x).contains(existing.id)
      }
  }

  property("pruneNonDescendants non-existing") = forAll(genTestData) {
    case (data, _, nonExisting) =>
      data.store.pruneNonDescendants(nonExisting.id)._2.isEmpty
  }
}
