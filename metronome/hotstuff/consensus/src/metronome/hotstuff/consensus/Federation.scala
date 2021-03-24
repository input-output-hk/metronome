package metronome.hotstuff.consensus

/** Collection of keys of the federation members. */
case class Federation[PKey](
    publicKeys: IndexedSeq[PKey]
) {
  private val publicKeySet = publicKeys.toSet

  /** Size of the federation, `n`. */
  val size: Int = publicKeys.size

  /** Maximum number of Byzantine nodes, `f`, so that `n >= 3*f+1`. */
  val maxFaulty: Int = (size - 1) / 3

  def contains(publicKey: PKey): Boolean =
    publicKeySet.contains(publicKey)

  def leaderOf(viewNumber: ViewNumber): PKey =
    publicKeys((viewNumber % size).toInt)
}
