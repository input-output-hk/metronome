package metronome.hotstuff.consensus

/** Strategy to pick the leader for a given view number from
  * federation of with a fixed size.
  */
trait LeaderSelection {

  /** Return the index of the federation member who should lead the view. */
  def leaderOf(viewNumber: ViewNumber, size: Int): Int
}

object LeaderSelection {
  object RoundRobin extends LeaderSelection {
    override def leaderOf(viewNumber: ViewNumber, size: Int): Int =
      (viewNumber % size).toInt
  }
}
