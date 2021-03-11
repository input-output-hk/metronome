package metronome.hotstuff.consensus.basic

/** All phases of the basic HotStuff protocol. */
sealed trait Phase

/** Subset of phases over which there can be vote and a Quorum Certificate. */
sealed trait VotingPhase extends Phase

object Phase {
  case object Prepare   extends VotingPhase
  case object PreCommit extends VotingPhase
  case object Commit    extends VotingPhase
  case object Decide    extends Phase

  /** In a given `phase`, what is the relative previous voting phase
    * from which we're expecting to receive a quorum.
    */
  def votingPhase(phase: Phase): Option[Phase] =
    phase match {
      case Phase.Prepare   => None
      case Phase.PreCommit => Some(Phase.Prepare)
      case Phase.Commit    => Some(Phase.PreCommit)
      case Phase.Decide    => Some(Phase.Commit)
    }
}
