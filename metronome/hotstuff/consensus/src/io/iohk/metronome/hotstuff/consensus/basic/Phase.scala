package io.iohk.metronome.hotstuff.consensus.basic

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

  /** Check that *within the same view* phase this phase precedes the other. */
  def isBefore(other: Phase): Boolean =
    (this, other) match {
      case (Prepare, PreCommit | Commit | Decide) => true
      case (PreCommit, Commit | Decide)           => true
      case (Commit, Decide)                       => true
      case _                                      => false
    }

  /** Check that *within the same view* this phase follows the other. */
  def isAfter(other: Phase): Boolean =
    (this, other) match {
      case (PreCommit, Prepare)                   => true
      case (Commit, Prepare | PreCommit)          => true
      case (Decide, Prepare | PreCommit | Commit) => true
      case _                                      => false
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
