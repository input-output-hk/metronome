package io.iohk.metronome.checkpointing.service.models

import io.iohk.ethereum.rlp._
import io.iohk.metronome.checkpointing.interpreter.models.Transaction
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.reflect.ClassTag
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary

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
      val ledger = Ledger(
        maybeLastCheckpoint = Some(
          sample[Transaction.CheckpointCandidate]
        ),
        proposerBlocks = Vector(
          sample[Transaction.ProposerBlock],
          sample[Transaction.ProposerBlock]
        )
      )

      override val decoded = ledger

      override val encoded =
        RLPList(     // Ledger
          RLPList(   // Option
            RLPList( // CheckpointCandidate
              RLPValue(ledger.maybeLastCheckpoint.get.value.toByteArray)
            )
          ),
          RLPList(   // Vector
            RLPList( // ProposerBlock
              RLPValue(ledger.proposerBlocks(0).value.toByteArray)
            ),
            RLPList(RLPValue(ledger.proposerBlocks(1).value.toByteArray))
          )
        )
    }
  }

  test {
    new Example[Transaction] {
      val transaction = sample[Transaction.ProposerBlock]

      override val decoded = transaction

      override val encoded =
        RLPList(                     // ProposerBlock
          RLPValue(Array(1.toByte)), // Tag
          RLPValue(transaction.value.toByteArray)
        )
    }
  }
}
