package io.iohk.metronome.crypto

/** An individual signature of a member with identity `K` over some content `H`,
  * represented by type `P`, e.g. `P` could be a single `Secp256k1Signature`
  * or a partial threshold signature of some sort.
  */
case class PartialSignature[K, H, P](sig: P)
