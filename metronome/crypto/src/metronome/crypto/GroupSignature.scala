package metronome.crypto

/** Group signature of members with identity `K` over some content `H`,
  * represented by type `S`.
  */
case class GroupSignature[K, H, G](sig: G)
