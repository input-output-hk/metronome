package metronome.hotstuff.consensus

/** Collection of keys of the federation members. */
case class Federation[K](
    publicKeys: IndexedSeq[K]
) {
  private val publicKeySet = publicKeys.toSet

  /** Size of the federation, `n`. */
  val size: Int =
    publicKeys.size

  /** Maximum number of Byzantine nodes, `f`, so that `n >= 3*f+1`. */
  val maxFaulty: Int = (size - 1) / 3

  def contains(publicKey: K): Boolean =
    publicKeySet.contains(publicKey)

  def leaderOf(viewNumber: ViewNumber): K =
    publicKeys((viewNumber.value % size).toInt)
}
