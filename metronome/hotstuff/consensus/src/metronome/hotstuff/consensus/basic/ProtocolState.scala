package metronome.hotstuff.consensus.basic

import cats.implicits._
import metronome.core.Validated
import metronome.crypto.{PartialSignature, GroupSignature}
import metronome.hotstuff.consensus.{ViewNumber, Federation}
import scala.concurrent.duration.FiniteDuration

/** Basic HotStuff protocol state machine.
  *
  * See https://arxiv.org/pdf/1803.05069.pdf
  */
case class ProtocolState[A <: Agreement: Block: Signing](
    viewNumber: ViewNumber,
    phase: Phase,
    publicKey: A#PKey,
    signingKey: A#SKey,
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
    // Votes gathered by the leader in this phase. They are guarenteed to be over the same content.
    votes: Set[Message.Vote[A]],
    // NewView messages gathered by the leader during the Prepare phase. Map so every sender can only give one.
    newViews: Map[A#PKey, Message.NewView[A]]
) {
  import Message._
  import Effect._
  import Event._
  import ProtocolState._
  import ProtocolError._

  val leader   = federation.leaderOf(viewNumber)
  val isLeader = leader == publicKey

  /** The leader has to collect `n-f` signatures into a Q.C. */
  def quorumSize = federation.size - federation.maxFaulty

  /** No state transition. */
  private def stay: Transition[A] =
    this -> Nil

  private def moveTo(phase: Phase): ProtocolState[A] =
    copy(
      viewNumber = if (phase == Phase.Prepare) viewNumber.next else viewNumber,
      phase = phase,
      votes = Set.empty,
      newViews = Map.empty
    )

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
      // TODO: If the block is empty, we could just repeat the agreement on
      // the previous Q.C. to simulate being idle, without timing out.
      val effects = broadcast {
        Prepare(viewNumber, e.block, e.highQC)
      }
      this -> effects
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

      case m: ReplicaMessage[_] if publicKey != expectedLeader =>
        Left(NotToLeader(e, expectedLeader))

      case m: Vote[_] if !Signing[A].validate(m) =>
        // TODO: Do we need to check that the `sender` is the key that signed?
        Left(InvalidVote(e.sender, m))

      case m: Quorum[_] if !Signing[A].validate(m.quorumCertificate) =>
        Left(InvalidQuorumCertificate(e.sender, m.quorumCertificate))

      case m: NewView[_] if !Signing[A].validate(m.prepareQC) =>
        Left(InvalidQuorumCertificate(e.sender, m.prepareQC))

      case m: Prepare[_] if !Signing[A].validate(m.highQC) =>
        Left(InvalidQuorumCertificate(e.sender, m.highQC))

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
  ): TransitionAttempt[A] =
    phase match {
      case Phase.Prepare =>
        matchingMsgAttempt(e) {
          case m: NewView[_] if m.viewNumber == viewNumber.prev && isLeader =>
            Right(addNewViewAndMaybeCreateBlock(e.sender, m))

          case m: Prepare[_] if matchingLeader(e) =>
            if (isSafe(m)) {
              // Check safe extension, vote Prepare, move to PreCommit.
              val blockHash = Block[A].blockHash(m.block)
              val effects = Seq(
                sendVote(Phase.Prepare, blockHash)
              )
              val next = moveTo(Phase.PreCommit).copy(
                preparedBlockHash = blockHash
              )
              Right(next -> effects)
            } else {
              Left(UnsafeExtension(e.sender, m))
            }
        }

      case Phase.PreCommit =>
        matchingMsg(e) {
          // Collect votes, broadcast Prepare Q.C.
          handleVotes orElse {
            case m: Quorum[_] if matchingLeader(e) && matchingQC(m) =>
              // Save prepareQC, vote pre-commit, move to Commit.
              val effects = Seq(
                sendVote(Phase.PreCommit, m.quorumCertificate.blockHash)
              )
              val next = moveTo(Phase.Commit).copy(
                prepareQC = m.quorumCertificate
              )
              next -> effects
          }
        }

      case Phase.Commit =>
        matchingMsg(e) {
          // Collect votes, broadcast Pre-Commit Q.C.
          handleVotes orElse {
            case m: Quorum[_] if matchingLeader(e) && matchingQC(m) =>
              // Save locked QC, vote commit, move to Decide.
              val effects = Seq(
                sendVote(Phase.Commit, m.quorumCertificate.blockHash)
              )
              val next = moveTo(Phase.Decide).copy(
                lockedQC = m.quorumCertificate
              )
              next -> effects
          }
        }

      case Phase.Decide =>
        matchingMsg(e) {
          // Collect votes, broadcast Commit Q.C.
          handleVotes orElse {
            case m: Quorum[_] if matchingLeader(e) && matchingQC(m) =>
              // Execute block, move to Prepare.
              handleNextView(NextView(viewNumber)) match {
                case (next, effects) =>
                  val withExec = ExecuteBlocks(
                    lastExecutedBlockHash,
                    m.quorumCertificate
                  ) +: effects

                  next -> withExec
              }
          }
        }
    }

  /** The leader's message handling is the same across all phases:
    * add the vote to the list; if we reached `n-f` then combine
    * into a Q.C. and broadcast.
    */
  private val handleVotes: PartialFunction[Message[A], Transition[A]] = {
    case v: Vote[_] if isLeader && matchingVote(v) =>
      addVoteAndMaybeBroadcastQC(v)

    case v: Vote[_] if isLeader && extravoteVote(v) =>
      stay
  }

  /** Try to match a message to expectations, or return Unexpected. */
  private def matchingMsg(e: MessageReceived[A])(
      pf: PartialFunction[Message[A], Transition[A]]
  ): TransitionAttempt[A] =
    matchingMsgAttempt(e) {
      pf.andThen(Right(_))
    }

  private def matchingMsgAttempt(e: MessageReceived[A])(
      pf: PartialFunction[Message[A], TransitionAttempt[A]]
  ): TransitionAttempt[A] =
    pf.lift(e.message).getOrElse(Left(Unexpected(e)))

  /** Check that a vote is compatible with our current expectations. */
  private def matchingVote(vote: Vote[A]): Boolean =
    viewNumber == vote.viewNumber &&
      Phase.votingPhase(phase).contains(vote.phase) &&
      preparedBlockHash == vote.blockHash

  /** Once the leader moves on to the next phase, it can still receive votes
    * for the previous one. These can be ignored, they are not unexpected.
    */
  private def extravoteVote(vote: Vote[A]): Boolean =
    viewNumber == vote.viewNumber &&
      preparedBlockHash == vote.blockHash &&
      !Phase.votingPhase(phase).contains(vote.phase)

  /** Check that a Q.C. is compatible with our current expectations. */
  private def matchingQC(m: Quorum[A]): Boolean =
    viewNumber == m.quorumCertificate.viewNumber &&
      Phase.votingPhase(phase).contains(m.quorumCertificate.phase) &&
      preparedBlockHash == m.quorumCertificate.blockHash

  /** Check that a message is coming from the view leader and is for the current phase. */
  private def matchingLeader(e: MessageReceived[A]): Boolean =
    e.message.viewNumber == viewNumber &&
      e.sender == federation.leaderOf(viewNumber)

  /** Broadcast a message from the leader to all replicas.
    *
    * This includes the leader sending a message to itself,
    * because the leader is a replica as well. The effect
    * system should take care that these messages don't
    * try to go over the network.
    */
  private def broadcast(m: Message[A]): Seq[Effect[A]] =
    federation.publicKeys.map { pk =>
      SendMessage(pk, m)
    }

  /** Produce a vote with the current view number. */
  private def vote(phase: VotingPhase, blockHash: A#Hash): Vote[A] = {
    val signature = Signing[A].sign(signingKey, phase, viewNumber, blockHash)
    Vote(viewNumber, phase, blockHash, signature)
  }

  private def sendVote(phase: VotingPhase, blockHash: A#Hash): SendMessage[A] =
    SendMessage(leader, vote(phase, blockHash))

  /** Check that the proposed new block extends the locked Q.C. (safety)
    * or that the Quorum Certificate is newer than the locked Q.C. (liveness).
    */
  private def isSafe(m: Prepare[A]): Boolean = {
    val valid = isExtension(m.block, m.highQC)
    val safe  = isExtension(m.block, lockedQC)
    val live  = m.highQC.viewNumber > lockedQC.viewNumber

    valid && (safe || live)
  }

  /** Check that a block extends from the one in the Q.C.
    *
    * Currently only allows direct parent-child relationship,
    * which means each leader is expected to create max 1 block
    * on top of the previous high Q.C.
    */
  private def isExtension(block: A#Block, qc: QuorumCertificate[A]): Boolean =
    qc.blockHash == Block[A].parentBlockHash(block)

  /** Register a new vote; if there are enough to form a new Q.C.,
    * do so and broadcast it.
    */
  private def addVoteAndMaybeBroadcastQC(vote: Vote[A]): Transition[A] = {
    // `matchingVote` made sure all votes are for the same content,
    // and `moveTo` clears the votes, so they should be uniform.
    val next = copy(votes = votes + vote)

    // Only make the quorum certificate once.
    val effects = if (next.votes.size == quorumSize) {
      val vs = votes.toSeq
      val qc = QuorumCertificate(
        phase = vs.head.phase,
        viewNumber = vs.head.viewNumber,
        blockHash = vs.head.blockHash,
        signature = Signing[A].combine(vs.map(_.signature))
      )
      broadcast {
        Quorum(viewNumber, qc)
      }
    } else Nil

    // The move to the next phase will be triggered when the Q.C. is delivered.
    next -> effects
  }

  /** Register a NewView from a replica; if there are enough, select the High Q.C. and create a block. */
  private def addNewViewAndMaybeCreateBlock(
      sender: A#PKey,
      newView: NewView[A]
  ): Transition[A] = {
    // We already checked that these are for the current view.
    val next = copy(newViews = newViews.updated(sender, newView))

    // Only make a block once.
    val effects = if (next.newViews.size == quorumSize) {
      List(
        CreateBlock(
          viewNumber,
          highQC = next.newViews.values.maxBy(_.viewNumber).prepareQC
        )
      )
    } else Nil

    // The move to the next phase will be triggered when the block is created.
    next -> effects
  }
}

object ProtocolState {

  /** The result of state transitions are the next state and some effects
    * that can be carried out in parallel.
    */
  type Transition[A <: Agreement] = (ProtocolState[A], Seq[Effect[A]])

  type TransitionAttempt[A <: Agreement] =
    Either[ProtocolError[A], Transition[A]]

  /** Return an initial set of effects; at the minimum the timeout for the first round. */
  def init[A <: Agreement](state: ProtocolState[A]): Seq[Effect[A]] =
    List(Effect.ScheduleNextView(state.viewNumber, state.timeout))
}
