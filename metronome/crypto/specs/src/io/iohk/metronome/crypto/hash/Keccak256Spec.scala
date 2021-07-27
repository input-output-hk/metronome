package io.iohk.metronome.crypto.hash

import scodec.bits._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class Keccak256Spec extends AnyFlatSpec with Matchers {
  behavior of "Keccak256"

  it should "hash empty data" in {
    Keccak256(
      "".getBytes
    ) shouldBe hex"c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
  }

  it should "hash non-empty data" in {
    Keccak256(
      "abc".getBytes
    ) shouldBe hex"4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45"
  }
}
