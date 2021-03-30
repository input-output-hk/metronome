package io.iohk.metronome.crypto.hash

import org.bouncycastle.crypto.digests.KeccakDigest
import scodec.bits.{BitVector, ByteVector}

object Keccak256 {
  def apply(data: Array[Byte]): Hash = {
    val output = new Array[Byte](32)
    val digest = new KeccakDigest(256)
    digest.update(data, 0, data.length)
    digest.doFinal(output, 0)
    Hash(ByteVector(output))
  }

  def apply(data: ByteVector): Hash =
    apply(data.toArray)

  def apply(data: BitVector): Hash =
    apply(data.toByteArray)
}
