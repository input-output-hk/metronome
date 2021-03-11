package metronome.hotstuff.consensus.basic

import metronome.core.Validated
import metronome.hotstuff.consensus.{ViewNumber, Federation}
import scala.concurrent.duration.FiniteDuration

/** Basic HotStuff protocol state with the following generic parameters:
  *
  * See https://arxiv.org/pdf/1803.05069.pdf
  */
case class ProtocolState[A <: Agreement: Block](
    viewNumber: ViewNumber,
    phase: Phase,
    ownPublicKey: A#PKey,
    federation: Federation[A#PKey],
    // Highest QC for which a replica voted Pre-Commit, because it received a Prepare Q.C. from the leader.
    prepareQC: QuorumCertificate[A],
    // Locked QC, for which a replica voted Commit, because it received a Pre-Commit Q.C. from leader.
    lockedQC: QuorumCertificate[A],
    // Hash of the block that was last decided upon.
    lastExecutedBlockHash: A#Hash,
    // Hash of the block the federation is currently voting on.
    preparedBlockHash: A#Hash,
    // Timeout for the view, so that it can be adjusted next time if necessary.
    timeout: FiniteDuration,
    // Votes gathered by the leader in this phase.
    votes: Set[Message.Vote[A]]
) {
  import Message._
  import Effect._
  import Event._
  import ProtocolState._
  import ProtocolError._

  val leader   = federation.leaderOf(viewNumber)
  val isLeader = leader == ownPublicKey

  /** No state transition. */
  private def stay: Transition[A] =
    this -> Nil

  private def moveTo(phase: Phase): ProtocolState[A] =
    copy(
      viewNumber = if (phase == Phase.Prepare) viewNumber + 1 else viewNumber,
      phase = phase,
      votes = Set.empty[Vote[A]]
    )

  /** Broadcast a message from the leader to all replicas.
    *
    * This includes the leader sending a message to itself,
    * because the leader is a replica as well. The effect
    * system should take care that these messages don't
    * try to go over the network.
    */
  private def broadcast(message: Message[A]): Seq[Effect[A]] =
    federation.publicKeys.map { pk =>
      SendMessage(pk, message)
    }

  /** The round has timed out; send `prepareQC` to the leader
    * of the next view and move to that view now.
    */
  def handleNextView(e: NextView): Transition[A] =
    if (e.viewNumber == viewNumber) {
      val next = moveTo(Phase.Prepare)

      val effects = Seq(
        SendMessage(next.leader, NewView(viewNumber, prepareQC)),
        ScheduleNextView(next.viewNumber, next.timeout)
      )

      next -> effects
    } else stay

  /** A block we asked the host system to create using `Effect.CreateBlock` is
    * ready to be broadcasted, if we're still in the same view.
    */
  def handleBlockCreated(e: BlockCreated[A]): Transition[A] =
    if (e.viewNumber == viewNumber && isLeader && phase == Phase.Prepare) {
      val next = copy(preparedBlockHash = Block[A].blockHash(e.block))

      val effects = broadcast {
        Prepare(viewNumber, e.block, e.highQC)
      }

      next -> effects
    } else stay

  /** Filter out messages that are completely invalid,
    * independent of the current phase and view number,
    * i.e. stateless validation.
    *
    * This check can be performed before for example the
    * block contents in the `Prepare` message are validated,
    * so that we don't waste time with spam.
    */
  def validateMessage(
      e: MessageReceived[A]
  ): Either[ProtocolError[A], Validated[MessageReceived[A]]] = {
    val expectedLeader = federation.leaderOf(e.message.viewNumber)

    e.message match {
      case _ if !federation.contains(e.sender) =>
        Left(NotFromFederation(e))

      case m: LeaderMessage[_] if e.sender != expectedLeader =>
        Left(NotFromLeader(e, expectedLeader))

      case m: ReplicaMessage[_] if ownPublicKey != expectedLeader =>
        Left(NotToLeader(e, expectedLeader))

      // TODO: Check that Vote signature is correct
      // TODO: Check that Quorum Certificate signature is correct
      // TODO: Check that the vote is about the block we are preparing

      case _ =>
        Right(Validated[MessageReceived[A]](e))
    }
  }

  /** Handle an incoming message that has already gone through partial validation:
    *
    * The sender is verified by the network layer and retrieved from the
    * lower level protocol message; we know the signatures are correct;
    * and the contents of any proposed block have been validated as well,
    * so they are safe to be voted on.
    *
    * Return the updated state and any effects to be carried out in response,
    * or an error, so that mismatches can be traced. Discrepancies can arise
    * from the state being different or have changed since the message originally
    * received.
    *
    * The structure of the method tries to match the pseudo code of `Algorithm 2`
    * in the HotStuff paper.
    */
  def handleMessage(
      e: Validated[MessageReceived[A]]
  ): Either[ProtocolError[A], Transition[A]] =
    phase match {
      case Phase.Prepare =>
        matchingMsg(e) {
          case m: NewView[_] if m.viewNumber == viewNumber - 1 && isLeader =>
            ??? // Select Highest Q.C., create block.
          case m: Prepare[_] if matchingLeader(e) =>
            ??? // Check safe extension, vote Prepare, move to pre-commit
        }

      case Phase.PreCommit =>
        matchingMsg(e) {
          case v: Vote[_] if isLeader && matchingVote(v) =>
            ??? // Collect votes, broadcast Prepare Q.C.
          case m: PreCommit[_]
              if matchingLeader(e) && matchingQC(m.prepareQC) =>
            ??? // Save prepareQC, vote pre-commit, move to commit
        }

      case Phase.Commit =>
        matchingMsg(e) {
          case v: Vote[_] if isLeader && matchingVote(v) =>
            ??? // Collect votes, broadcast Pre-Commit Q.C.
          case m: Commit[_] if matchingLeader(e) && matchingQC(m.precommitQC) =>
            ??? // Save locked QC, vote commit, move to decide
        }

      case Phase.Decide =>
        matchingMsg(e) {
          case v: Vote[_] if isLeader && matchingVote(v) =>
            ??? // Collect votes, broadcast Commit Q.C.
          case m: Decide[_] if matchingLeader(e) && matchingQC(m.commitQC) =>
            ??? // Execute block, move to Prepare
        }
    }

  /** Try to match a message to expectations, or return Unexpected. */
  private def matchingMsg(e: MessageReceived[A])(
      pf: PartialFunction[Message[A], Transition[A]]
  ): Either[ProtocolError[A], Transition[A]] =
    pf.lift(e.message).map(Right(_)).getOrElse(Left(Unexpected(e)))

  /** Check that a vote is compatible with our current expectations. */
  private def matchingVote(vote: Vote[A]): Boolean =
    viewNumber == vote.viewNumber &&
      prevPhase.contains(vote.phase) &&
      preparedBlockHash == vote.blockHash

  /** Check that a Q.C. is compatible with our current expectations. */
  private def matchingQC(qc: QuorumCertificate[A]): Boolean =
    viewNumber == qc.viewNumber &&
      prevPhase.contains(qc.phase) &&
      preparedBlockHash == qc.blockHash

  /** Check that a message is coming from the view leader and is for the current phase. */
  private def matchingLeader(e: MessageReceived[A]): Boolean =
    e.message.viewNumber == viewNumber &&
      e.message.phase == phase &&
      e.sender == federation.leaderOf(viewNumber)

  /** Which phase does the current one look for in votes and quorum certificates. */
  private def prevPhase: Option[Phase] =
    phase match {
      case Phase.Prepare   => None
      case Phase.PreCommit => Some(Phase.Prepare)
      case Phase.Commit    => Some(Phase.PreCommit)
      case Phase.Decide    => Some(Phase.Commit)
    }
}

object ProtocolState {

  /** The result of state transitions are the next state and some effects
    * that can be carried out in parallel.
    */
  type Transition[A <: Agreement] = (ProtocolState[A], Seq[Effect[A]])

  /** Return an initial set of effects; at the minimum the timeout for the first round. */
  def init[A <: Agreement](state: ProtocolState[A]): Seq[Effect[A]] =
    List(Effect.ScheduleNextView(state.viewNumber, state.timeout))

}
