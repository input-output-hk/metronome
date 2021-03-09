package metronome.crypto

/** An individual signature of a member with identity `K` over some content `H`,
  * represented by type `P`.
  */
case class PartialSignature[K, H, P](sig: P)
