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
}
