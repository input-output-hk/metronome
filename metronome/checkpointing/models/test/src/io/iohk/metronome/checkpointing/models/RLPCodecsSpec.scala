package io.iohk.metronome.checkpointing.models

import cats.data.NonEmptyList
import io.iohk.ethereum.crypto.ECDSASignature
import io.iohk.ethereum.rlp._
import io.iohk.metronome.crypto.GroupSignature
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.hotstuff.consensus.basic.{Phase, QuorumCertificate}
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.reflect.ClassTag

/** Concrete examples of RLP encoding, so we can make sure the structure is what we expect.
  *
  * Complements `RLPCodecsProps` which works with arbitrary data.
  */
class RLPCodecsSpec extends AnyFlatSpec with Matchers {
  import ArbitraryInstances._
  import RLPCodecs._

  def sample[T: Arbitrary] = arbitrary[T].sample.get

  // Structrual equality checker for RLPEncodeable.
  // It has different wrappers for items based on whether it was hand crafted or generated
  // by codecs, and the RLPValue has mutable arrays inside.
  implicit val eqRLPList = new Equality[RLPEncodeable] {
    override def areEqual(a: RLPEncodeable, b: Any): Boolean =
      (a, b) match {
        case (a: RLPList, b: RLPList) =>
          a.items.size == b.items.size && a.items.zip(b.items).forall {
            case (a, b) =>
              areEqual(a, b)
          }
        case (a: RLPValue, b: RLPValue) =>
          a.bytes.sameElements(b.bytes)
        case other =>
          false
      }
  }

  abstract class Example[T: RLPCodec: ClassTag] {
    def decoded: T
    def encoded: RLPEncodeable

    def name =
      s"RLPCodec[${implicitly[ClassTag[T]].runtimeClass.getSimpleName}]"

    def encode = RLPEncoder.encode(decoded)
    def decode = RLPDecoder.decode[T](encoded)
  }

  def exampleBehavior[T](example: Example[T]) = {
    it should "encode the example value to the expected RLP data" in {
      example.encode shouldEqual example.encoded
    }

    it should "decode the example RLP data to the expected value" in {
      example.decode shouldEqual example.decoded
    }
  }

  def test[T](example: Example[T]) = {
    example.name should behave like exampleBehavior(example)
  }

  test {
    new Example[Ledger] {
      override val decoded = Ledger(
        maybeLastCheckpoint = Some(
          sample[Transaction.CheckpointCandidate]
        ),
        proposerBlocks = Vector(
          sample[Transaction.ProposerBlock],
          sample[Transaction.ProposerBlock]
        )
      )

      override val encoded =
        RLPList(     // Ledger
          RLPList(   // Option
            RLPList( // CheckpointCandidate
              RLPValue(decoded.maybeLastCheckpoint.get.value.toByteArray)
            )
          ),
          RLPList(   // Vector
            RLPList( // ProposerBlock
              RLPValue(decoded.proposerBlocks(0).value.toByteArray)
            ),
            RLPList(RLPValue(decoded.proposerBlocks(1).value.toByteArray))
          )
        )
    }
  }

  test {
    new Example[Transaction] {
      override val decoded = sample[Transaction.ProposerBlock]

      override val encoded =
        RLPList(                     // ProposerBlock
          RLPValue(Array(1.toByte)), // Tag
          RLPValue(decoded.value.toByteArray)
        )
    }
  }

  test {
    new Example[CheckpointCertificate] {
      val decoded = CheckpointCertificate(
        headers = NonEmptyList.of(
          sample[Block.Header],
          sample[Block.Header]
        ),
        checkpoint = sample[Transaction.CheckpointCandidate],
        proof = MerkleTree.Proof(
          leafIndex = 2,
          leafCount = 4,
          siblingPath = Vector(sample[MerkleTree.Hash], sample[MerkleTree.Hash])
        ),
        commitQC = QuorumCertificate[CheckpointingAgreement](
          phase = Phase.Commit,
          viewNumber = ViewNumber(10),
          blockHash = sample[Block.Header.Hash],
          signature = GroupSignature(
            List(
              sample[ECDSASignature],
              sample[ECDSASignature]
            )
          )
        )
      )

      override val encoded =
        RLPList(     // CheckpointCertificate
          RLPList(   // NonEmptyList
            RLPList( // BlockHeader
              RLPValue(decoded.headers.head.parentHash.toArray),
              RLPValue(decoded.headers.head.preStateHash.toArray),
              RLPValue(decoded.headers.head.postStateHash.toArray),
              RLPValue(decoded.headers.head.bodyHash.toArray),
              RLPValue(decoded.headers.head.contentMerkleRoot.toArray)
            ),
            RLPList( // BlockHeader
              RLPValue(decoded.headers.last.parentHash.toArray),
              RLPValue(decoded.headers.last.preStateHash.toArray),
              RLPValue(decoded.headers.last.postStateHash.toArray),
              RLPValue(decoded.headers.last.bodyHash.toArray),
              RLPValue(decoded.headers.last.contentMerkleRoot.toArray)
            )
          ),
          RLPList( // CheckpointCandidate
            RLPValue(decoded.checkpoint.value.toByteArray)
          ),
          RLPList( // Proof
            RLPValue(Array(decoded.proof.leafIndex.toByte)),
            RLPValue(Array(decoded.proof.leafCount.toByte)),
            RLPList( // siblingPath
              RLPValue(decoded.proof.siblingPath.head.toArray),
              RLPValue(decoded.proof.siblingPath.last.toArray)
            )
          ),
          RLPList(                      // QuorumCertificate
            RLPValue(Array(3.toByte)),  // Commit
            RLPValue(Array(10.toByte)), // ViewNumber
            RLPValue(decoded.commitQC.blockHash.toArray),
            RLPList(      // GroupSignature
              RLPList(    // sig
                RLPValue( // ECDSASignature
                  decoded.commitQC.signature.sig.head.toBytes.toArray[Byte]
                ),
                RLPValue(
                  decoded.commitQC.signature.sig.last.toBytes.toArray[Byte]
                )
              )
            )
          )
        )
    }
  }
}
