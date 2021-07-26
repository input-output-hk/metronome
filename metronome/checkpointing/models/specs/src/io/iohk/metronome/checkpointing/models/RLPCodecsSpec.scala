package io.iohk.metronome.checkpointing.models

import cats.data.NonEmptyList
import io.iohk.ethereum.crypto.ECDSASignature
import io.iohk.ethereum.rlp._
import io.iohk.ethereum.rlp
import io.iohk.metronome.crypto.GroupSignature
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.hotstuff.consensus.basic.{Phase, QuorumCertificate}
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import java.nio.file.{Files, Path, StandardOpenOption}
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.reflect.ClassTag
import scodec.bits.BitVector

/** Concrete examples of RLP encoding, so we can make sure the structure is what we expect.
  *
  * Complements `RLPCodecsProps` which works with arbitrary data.
  */
class RLPCodecsSpec extends AnyFlatSpec with Matchers {
  import ArbitraryInstances._
  import RLPCodecs._

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
        case _ =>
          false
      }
  }

  abstract class Example[T: RLPCodec: ClassTag] {
    def decoded: T
    def encoded: RLPEncodeable

    def name =
      s"RLPCodec[${implicitly[ClassTag[T]].runtimeClass.getSimpleName}]"

    def encode: RLPEncodeable = RLPEncoder.encode(decoded)
    def decode: T             = RLPDecoder.decode[T](encoded)

    def decode(bytes: BitVector): T =
      rlp.decode[T](bytes.toByteArray)
  }

  def exampleBehavior[T](example: Example[T]) = {
    it should "encode the example value to the expected RLP data" in {
      example.encode shouldEqual example.encoded
    }

    it should "decode the example RLP data to the expected value" in {
      example.decode shouldEqual example.decoded
    }
  }

  /** When the example is first executed, create a golden file that we can
    * check in with the code for future reference, and to detect any regression.
    *
    * If there are intentional changes, just delete it and let it be recreated.
    * This could be used as a starter for implemnting the same format in a
    * different language.
    *
    * The String format is not expected to match other implementations, but it's
    * easy enough to read, and should be as good as a hard coded example either
    * in code or a README file.
    */
  def goldenBehavior[T](example: Example[T]) = {

    def resourcePath(extension: String): Path = {
      val goldenPath = Path.of(getClass.getResource("/golden").toURI)
      goldenPath.resolve(s"${example.name}.${extension}")
    }

    def maybeCreateResource(path: Path, content: => String) = {
      if (!Files.exists(path)) {
        Files.writeString(path, content, StandardOpenOption.CREATE_NEW)
      }
    }

    val goldenRlpPath = resourcePath("rlp")
    val goldenTxtPath = resourcePath("txt")

    maybeCreateResource(
      goldenRlpPath,
      BitVector(rlp.encode(example.encoded)).toHex
    )

    maybeCreateResource(
      goldenTxtPath,
      example.decoded.toString
    )

    it should "decode the golden RLP content to a value that matches the golden String" in {
      val goldenRlp = BitVector.fromHex(Files.readString(goldenRlpPath)).get
      val goldenTxt = Files.readString(goldenTxtPath)

      example.decode(goldenRlp).toString shouldBe goldenTxt
    }
  }

  def test[T](example: Example[T]) = {
    example.name should behave like exampleBehavior(example)
    example.name should behave like goldenBehavior(example)
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
              RLPValue(
                rlp.RLPImplicits.longEncDec
                  .encode(decoded.headers.head.height)
                  .bytes
              ),
              RLPValue(decoded.headers.head.postStateHash.toArray),
              RLPValue(decoded.headers.head.contentMerkleRoot.toArray)
            ),
            RLPList( // BlockHeader
              RLPValue(decoded.headers.last.parentHash.toArray),
              RLPValue(
                rlp.RLPImplicits.longEncDec
                  .encode(decoded.headers.last.height)
                  .bytes
              ),
              RLPValue(decoded.headers.last.postStateHash.toArray),
              RLPValue(decoded.headers.last.contentMerkleRoot.toArray)
            )
          ),
          RLPList( // CheckpointCandidate
            RLPValue(decoded.checkpoint.value.toByteArray)
          ),
          RLPList( // Proof
            RLPValue(Array(decoded.proof.leafIndex.toByte)),
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
