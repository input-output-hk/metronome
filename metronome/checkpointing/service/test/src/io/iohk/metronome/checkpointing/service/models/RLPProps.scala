package io.iohk.metronome.checkpointing.service.models

import io.iohk.ethereum.rlp
import io.iohk.ethereum.rlp.RLPCodec
import org.scalacheck._
import org.scalacheck.Prop.forAll

object RLPCodecsProps extends Properties("RLPCodecs") {
  import ArbitraryInstances._
  import RLPCodecs._

  def roundtrip[T: RLPCodec: Arbitrary]: Prop = forAll { (value0: T) =>
    val bytes  = rlp.encode(value0)
    val value1 = rlp.decode[T](bytes)
    value0 == value1
  }

  property("ledger") = roundtrip[Ledger]
}
