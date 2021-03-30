package io.iohk.metronome.crypto

/** Group signature of members with identity `K` over some content `H`,
  * represented by type `G`, e.g. `G` could be a `List[Secp256k1Signature]`
  * or a single combined threshold signature of some sort.
  */
case class GroupSignature[K, H, G](sig: G)
