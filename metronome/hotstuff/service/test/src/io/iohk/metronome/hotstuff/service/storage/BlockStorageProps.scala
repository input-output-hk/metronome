package io.iohk.metronome.hotstuff.service.storage

import cats.implicits._
import io.iohk.metronome.storage.{KVCollection, KVStoreState}
import io.iohk.metronome.hotstuff.consensus.basic.{Agreement, Block => BlockOps}
import java.util.UUID
import org.scalacheck._
import org.scalacheck.Prop.forAll
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

  /** Generate a block tree from a genesis block. */
  def genBlockTree: Gen[List[TestBlock]] =
    genBlockTree(parentId = "")

  case class TestData(
      tree: List[TestBlock],
      store: TestKVStore.Store
  )

  /** Initialise a storage with a chain. */
  def genTestData: Gen[TestData] =
    genBlockTree.map { blocks =>
      val insert = blocks.map(TestBlockStorage.put).sequence
      val store  = TestKVStore.compile(insert).runS(Map.empty).value
      TestData(blocks, store)
    }

  property("put") = forAll(genTestData, genBlock) { case (data, block) =>
    val p = TestBlockStorage.put(block)
    val s = TestKVStore.compile(p).runS(data.store).value

    s(Namespace.Blocks)(block.id) == block
    s(Namespace.BlockToParent)(block.id) == block.parentId
  }
}
