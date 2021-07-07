package io.iohk.metronome.examples.robot

import io.iohk.metronome.crypto.hash.{Hash, Keccak256}
import scodec.Codec

package object models {
  def codecHash[T: Codec](data: T): Hash =
    Keccak256(implicitly[Codec[T]].encode(data).require)
}
