package io.iohk.metronome.checkpointing.models

import io.iohk.ethereum.crypto.ECDSASignature
import io.iohk.ethereum.rlp
import io.iohk.ethereum.rlp.RLPCodec
import org.scalacheck._
import org.scalacheck.Prop.forAll
import scala.reflect.ClassTag

object RLPCodecsProps extends Properties("RLPCodecs") {
  import ArbitraryInstances._
  import RLPCodecs._

  /** Test that encoding to and decoding from RLP preserves the value. */
  def propRoundTrip[T: RLPCodec: Arbitrary: ClassTag] =
    property(implicitly[ClassTag[T]].runtimeClass.getSimpleName) = forAll {
      (value0: T) =>
        val bytes  = rlp.encode(value0)
        val value1 = rlp.decode[T](bytes)
        value0 == value1
    }

  propRoundTrip[Ledger]
  propRoundTrip[Transaction]
  propRoundTrip[Block]
  propRoundTrip[ECDSASignature]
  propRoundTrip[CheckpointCertificate]
}
