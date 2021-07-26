package io.iohk.metronome.checkpointing.interpreter.codecs

import io.iohk.metronome.checkpointing.interpreter.messages.{
  ArbitraryInstances,
  InterpreterMessage
}
import org.scalacheck._
import org.scalacheck.Prop.forAll
import scala.reflect.ClassTag
import scodec.Codec

object DefaultInterpreterCodecsProps
    extends Properties("DefaultInterpreterCodecs") {
  import ArbitraryInstances._
  import DefaultInterpreterCodecs._
  import InterpreterMessage._

  /** Test that encoding to and decoding from RLP preserves the value. */
  def propRoundTrip[T: Codec: Arbitrary: ClassTag] =
    property(implicitly[ClassTag[T]].runtimeClass.getSimpleName) = forAll {
      (value0: T) =>
        val bytes  = Codec[T].encode(value0).require
        val value1 = Codec[T].decodeValue(bytes).require
        value0 == value1
    }

  propRoundTrip[NewProposerBlockRequest]
  propRoundTrip[NewCheckpointCandidateRequest]
  propRoundTrip[CreateBlockBodyRequest]
  propRoundTrip[CreateBlockBodyResponse]
  propRoundTrip[ValidateBlockBodyRequest]
  propRoundTrip[ValidateBlockBodyResponse]
  propRoundTrip[NewCheckpointCertificateRequest]
  propRoundTrip[InterpreterMessage]
}
