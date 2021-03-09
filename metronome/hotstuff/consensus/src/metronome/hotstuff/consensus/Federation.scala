package metronome.hotstuff.consensus

/** Collection of keys of the federation members. */
case class Federation[K](
    publicKeys: IndexedSeq[K]
)
