package metronome.hotstuff.consensus.basic

/** All phases of the basic HotStuff protocol. */
sealed trait Phase {
  import Phase._
  def next: Phase =
    this match {
      case Prepare   => PreCommit
      case PreCommit => Commit
      case Commit    => Decide
      case Decide    => Prepare
    }

  def prev: Phase =
    this match {
      case Prepare   => Decide
      case PreCommit => Prepare
      case Commit    => PreCommit
      case Decide    => Commit
    }
}

/** Subset of phases over which there can be vote and a Quorum Certificate. */
sealed trait VotingPhase extends Phase

object Phase {
  case object Prepare   extends VotingPhase
  case object PreCommit extends VotingPhase
  case object Commit    extends VotingPhase
  case object Decide    extends Phase
}
