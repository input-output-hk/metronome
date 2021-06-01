package io.iohk.metronome.hotstuff.consensus.basic

import io.iohk.metronome.crypto.{GroupSignature, PartialSignature}
import io.iohk.metronome.hotstuff.consensus.{
  ViewNumber,
  Federation,
  LeaderSelection
}
import org.scalacheck.commands.Commands
import org.scalacheck.{Properties, Gen, Prop}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.{propBoolean, all, falsified}
import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.util.{Try, Failure, Success}

object ProtocolStateProps extends Properties("Basic HotStuff") {

  property("protocol") = ProtocolStateCommands.property()

}

/** State machine tests for the Basic HotStuff protocol.
  *
  * The `Model` class has enough reflection of the state so that we can generate valid
  * and invalid commands using `genCommand`. Each `Command`, has its individual post-condition
  * check comparing the model state to the actual protocol results.
  */
object ProtocolStateCommands extends Commands {

  case class TestBlock(
      blockHash: Int,
      parentBlockHash: Int,
      command: String
  )

  object TestAgreement extends Agreement {
    type Block = TestBlock
    type Hash  = Int
    type PSig  = Long
    type GSig  = Seq[Long]
    type PKey  = Int
    type SKey  = Int
  }
  type TestAgreement = TestAgreement.type

  val genesis =
    TestBlock(blockHash = 0, parentBlockHash = -1, command = "")

  val genesisQC = QuorumCertificate[TestAgreement, Phase.Prepare](
    phase = Phase.Prepare,
    viewNumber = ViewNumber(0),
    blockHash = genesis.blockHash,
    signature = GroupSignature(Nil)
  )

  implicit val block: Block[TestAgreement] = new Block[TestAgreement] {
    override def blockHash(b: TestBlock)       = b.blockHash
    override def parentBlockHash(b: TestBlock) = b.parentBlockHash
    override def height(b: TestBlock): Long    = 0 // Not used by this model.
    override def isValid(b: TestBlock)         = true
  }

  implicit val leaderSelection = LeaderSelection.Hashing

  // Going to use publicKey == -1 * signingKey.
  def mockSigningKey(pk: TestAgreement.PKey): TestAgreement.SKey = -1 * pk

  // Mock signatures.
  implicit val mockSigning: Signing[TestAgreement] =
    new Signing[TestAgreement] {
      private def hash(
          phase: VotingPhase,
          viewNumber: ViewNumber,
          blockHash: TestAgreement.Hash
      ): TestAgreement.Hash =
        (phase, viewNumber, blockHash).hashCode

      private def isGenesis(
          viewNumber: ViewNumber,
          blockHash: TestAgreement.Hash
      ): Boolean =
        viewNumber == genesisQC.viewNumber &&
          blockHash == genesisQC.blockHash

      private def sign(
          sk: TestAgreement.SKey,
          h: TestAgreement.Hash
      ): TestAgreement.PSig =
        h + sk

      private def unsign(
          s: TestAgreement.PSig,
          h: TestAgreement.Hash
      ): TestAgreement.PKey =
        ((s - h) * -1).toInt

      override def sign(
          signingKey: TestAgreement#SKey,
          phase: VotingPhase,
          viewNumber: ViewNumber,
          blockHash: TestAgreement.Hash
      ): Signing.PartialSig[TestAgreement] = {
        val h = hash(phase, viewNumber, blockHash)
        val s = sign(signingKey, h)
        PartialSignature(s)
      }

      override def combine(
          signatures: Seq[Signing.PartialSig[TestAgreement]]
      ): Signing.GroupSig[TestAgreement] =
        GroupSignature(signatures.map(_.sig))

      override def validate(
          publicKey: TestAgreement.PKey,
          signature: Signing.PartialSig[TestAgreement],
          phase: VotingPhase,
          viewNumber: ViewNumber,
          blockHash: TestAgreement.Hash
      ): Boolean = {
        val h = hash(phase, viewNumber, blockHash)
        publicKey == unsign(signature.sig, h)
      }

      override def validate(
          federation: Federation[TestAgreement.PKey],
          signature: Signing.GroupSig[TestAgreement],
          phase: VotingPhase,
          viewNumber: ViewNumber,
          blockHash: TestAgreement.Hash
      ): Boolean = {
        if (isGenesis(viewNumber, blockHash)) {
          signature.sig.isEmpty
        } else {
          val h = hash(phase, viewNumber, blockHash)

          signature.sig.size == federation.quorumSize &&
          signature.sig.forall { sig =>
            federation.publicKeys.exists { publicKey =>
              publicKey == unsign(sig, h)
            }
          }
        }
      }
    }

  case class Model(
      n: Int,
      f: Int,
      viewNumber: ViewNumber,
      phase: Phase,
      federation: Vector[TestAgreement.PKey],
      ownIndex: Int,
      votesFrom: Set[TestAgreement.PKey],
      newViewsFrom: Set[TestAgreement.PKey],
      newViewsHighQC: QuorumCertificate[TestAgreement, Phase.Prepare],
      prepareQCs: List[QuorumCertificate[TestAgreement, Phase.Prepare]],
      maybeBlockHash: Option[TestAgreement.Hash]
  ) {
    def publicKey = federation(ownIndex)

    // Using a signing key that works with the mock validation.
    def signingKey = mockSigningKey(publicKey)

    def leaderIndex = leaderSelection.leaderOf(viewNumber, n)
    def isLeader    = leaderIndex == ownIndex
    def leader      = federation(leaderIndex)

    def quorumSize = (n + f) / 2 + 1
  }

  // Keep a variable state in our System Under Test.
  class Protocol(var state: ProtocolState[TestAgreement])

  type Sut   = Protocol
  type State = Model

  @nowarn
  override def canCreateNewSut(
      newState: State,
      initSuts: Traversable[State],
      runningSuts: Traversable[Sut]
  ): Boolean = true

  override def initialPreCondition(state: State): Boolean =
    state.viewNumber == 1 &&
      state.phase == Phase.Prepare &&
      state.votesFrom.isEmpty &&
      state.newViewsFrom.isEmpty

  override def newSut(state: State): Sut = {
    import Phase._
    new Protocol(
      ProtocolState[TestAgreement](
        viewNumber = ViewNumber(state.viewNumber),
        phase = state.phase,
        publicKey = state.publicKey,
        signingKey = state.signingKey,
        federation = Federation(state.federation, state.f)
          .getOrElse(sys.error("Invalid federation!")),
        prepareQC = genesisQC,
        lockedQC = genesisQC.copy[TestAgreement, PreCommit](phase = PreCommit),
        commitQC = genesisQC.copy[TestAgreement, Commit](phase = Commit),
        preparedBlock = genesis,
        timeout = 10.seconds,
        votes = Set.empty,
        newViews = Map.empty
      )
    )
  }

  override def destroySut(sut: Sut): Unit = ()

  override def genInitialState: Gen[State] =
    for {
      n <- Gen.choose(1, 10)
      f <- Gen.choose(0, (n - 1) / 3)

      ownIndex <- Gen.choose(0, n - 1)

      // Create unique keys.
      publicKeys <- Gen
        .listOfN(n, Gen.posNum[Int])
        .map { ns =>
          ns.tail.scan(ns.head)(_ + _)
        }
        .retryUntil(_.size == n)

    } yield Model(
      n,
      f,
      viewNumber = ViewNumber(1),
      phase = Phase.Prepare,
      federation = publicKeys.toVector,
      ownIndex = ownIndex,
      votesFrom = Set.empty,
      newViewsFrom = Set.empty,
      newViewsHighQC = genesisQC,
      prepareQCs = List(genesisQC),
      maybeBlockHash = None
    )

  /** Generate valid and invalid commands depending on state.
    *
    * Invalid commands are marked as such, so we don't have to repeat validations here
    * to tell what we expect the response to be. We can send invalid commands from up
    * to `f` Bzyantine members of the federation. The rest should be honest, but they
    * might still send commands which are delivered in a different state, e.g. because
    * they didn't have the data available to validate a proposal.
    */
  override def genCommand(state: State): Gen[Command] =
    Gen.frequency(
      7 -> genValid(state),
      2 -> genInvalid(state),
      1 -> genTimeout(state)
    )

  def fail(msg: String) = msg |: falsified

  def votingPhaseFor(phase: Phase): Option[VotingPhase] =
    phase match {
      case Phase.Prepare   => None
      case Phase.PreCommit => Some(Phase.Prepare)
      case Phase.Commit    => Some(Phase.PreCommit)
      case Phase.Decide    => Some(Phase.Commit)
    }

  def genTimeout(state: State): Gen[NextViewCmd] =
    Gen.const(NextViewCmd(state.viewNumber))

  /** Geneerate a valid input for the givens state. */
  def genValid(state: State): Gen[Command] = {
    val usables: List[Gen[Command]] =
      List(
        // The leader may receive NewView any time.
        genValidNewView(state) ->
          state.isLeader,
        // The leader can get a block generated by the host system in Prepare.
        genValidBlock(state) ->
          (state.phase == Phase.Prepare && state.isLeader && state.maybeBlockHash.isEmpty),
        // Replicas can get a Prepared block in Prepare (for the leader this should match the created block).
        genValidPrepare(state) ->
          (state.phase == Phase.Prepare &&
            (state.isLeader && state.maybeBlockHash.isDefined ||
              !state.isLeader && state.maybeBlockHash.isEmpty)),
        // The leader can get votes on the block it created, except in Prepare.
        genValidVote(state) ->
          (state.phase != Phase.Prepare && state.isLeader && state.maybeBlockHash.isDefined),
        // Replicas can get a Quroum on the block that was Prepared, except in Prepare.
        genValidQuorum(state) ->
          (state.phase != Phase.Prepare && state.maybeBlockHash.isDefined)
      ).collect {
        case (gen, usable) if usable => gen
      }

    usables match {
      case Nil                => genTimeout(state)
      case one :: Nil         => one
      case one :: two :: rest => Gen.oneOf(one, two, rest: _*)
    }
  }

  /** Take an valid command and turn it invalid. */
  def genInvalid(state: State): Gen[Command] = {
    def nextVoting(phase: Phase): VotingPhase = {
      phase.next match {
        case p: VotingPhase => p
        case p              => nextVoting(p)
      }
    }

    def invalidateHash(h: TestAgreement.Hash) = h * 2 + 1
    def invalidateSig(s: TestAgreement.PSig)  = s * 2 + 1
    def invalidateViewNumber(v: ViewNumber)   = ViewNumber(v + 1000)
    def invalidSender                         = state.federation.sum + 1

    def invalidateQC[P <: VotingPhase](
        qc: QuorumCertificate[TestAgreement, P]
    ): Gen[QuorumCertificate[TestAgreement, P]] = {
      Gen.oneOf(
        genLazy(
          qc.withBlockHash(invalidateHash(qc.blockHash))
        ),
        // Now that the compiler (and codecs) check that we're getting the right message,
        // we shouldn't encounter an unexpected phase in a field like `highQC`,
        // so this kind of test is no longer needed and would fail with an assertion error
        // when it's coerced into a type it doesn't match.
        // genLazy(
        //   qc.withPhase[VotingPhase](nextVoting(qc.votingPhase))
        // ).suchThat(_.blockHash != genesisQC.blockHash),
        genLazy(
          qc.withViewNumber(invalidateViewNumber(qc.viewNumber))
        ),
        genLazy(
          qc.withSignature(
            // The quorum cert has no items, so add one to make it different.
            qc.signature.copy(sig = 0L +: qc.signature.sig.map(invalidateSig))
          )
        )
      )
    }

    implicit class StringOps(label: String) {
      def `!`(gen: Gen[MessageCmd]): Gen[InvalidCmd] =
        gen.map(cmd => InvalidCmd(label, cmd, isEarly = label == "viewNumber"))
    }

    genValid(state) flatMap {
      case msg: MessageCmd =>
        msg match {
          case cmd @ NewViewCmd(_, m) =>
            Gen.oneOf(
              "sender" ! genLazy(cmd.copy(sender = invalidSender)),
              "viewNumber" ! genLazy(
                cmd.copy(message =
                  m.copy(viewNumber = invalidateViewNumber(m.viewNumber))
                )
              ),
              "prepareQC" ! invalidateQC(m.prepareQC).map { qc =>
                cmd.copy(message = m.copy(prepareQC = qc))
              }
            )

          case cmd @ PrepareCmd(_, m) =>
            Gen.oneOf(
              "sender" ! genLazy(cmd.copy(sender = invalidSender)),
              "viewNumber" ! genLazy(
                cmd.copy(message = m.copy(viewNumber = m.viewNumber.next))
              ),
              "parentBlockHash" ! genLazy(
                cmd.copy(message =
                  m.copy[TestAgreement](block =
                    m.block
                      .copy(parentBlockHash =
                        invalidateHash(m.block.parentBlockHash)
                      )
                  )
                )
              ),
              "highQC" ! invalidateQC(m.highQC).map { qc =>
                cmd.copy(message = m.copy(highQC = qc))
              }
            )

          case cmd @ VoteCmd(_, m) =>
            Gen.oneOf(
              "sender" ! genLazy(cmd.copy(sender = invalidSender)),
              "viewNumber" ! genLazy(
                cmd.copy(message =
                  m.copy[TestAgreement](viewNumber =
                    invalidateViewNumber(m.viewNumber)
                  )
                )
              ),
              "phase" ! genLazy(
                cmd.copy(message =
                  m.copy[TestAgreement](phase = nextVoting(m.phase))
                )
              ),
              "blockHash" ! genLazy(
                cmd.copy(message =
                  m.copy[TestAgreement](blockHash = invalidateHash(m.blockHash))
                )
              ),
              "signature" ! genLazy(
                cmd.copy(message =
                  m.copy[TestAgreement](signature =
                    m.signature.copy(sig = invalidateSig(m.signature.sig))
                  )
                )
              )
            )

          case cmd @ QuorumCmd(_, m) =>
            Gen.oneOf(
              "sender" ! genLazy(cmd.copy(sender = invalidSender)),
              "quorumCertificate" ! invalidateQC(m.quorumCertificate)
                .map { qc =>
                  cmd.copy(message = m.copy(quorumCertificate = qc))
                }
            )
        }

      // Leave anything else alone.
      case other => Gen.const(other)
    }
  }

  /** A constant expression, but only evaluated if the generator is chosen,
    * which allows us to have conditions attached to it.
    */
  def genLazy[A](a: => A): Gen[A] = Gen.lzy(Gen.const(a))

  /** Replica sends a new view with an arbitrary prepare QC. */
  def genValidNewView(state: State): Gen[NewViewCmd] =
    for {
      s  <- Gen.oneOf(state.federation)
      qc <- Gen.oneOf(state.prepareQCs)
      m = Message.NewView(ViewNumber(state.viewNumber - 1), qc)
    } yield NewViewCmd(s, m)

  /** Leader creates a valid block on top of the saved High Q.C. */
  def genValidBlock(state: State): Gen[BlockCreatedCmd] =
    for {
      c <- arbitrary[String]
      h <- genHash
      qc = state.prepareQCs.head // So that it's a safe extension.
      p  = qc.blockHash
      b  = TestBlock(h, p, c)
      e = Event
        .BlockCreated[TestAgreement](state.viewNumber, b, qc)
    } yield BlockCreatedCmd(e)

  /** Leader sends a valid Prepare command with the generated block. */
  def genValidPrepare(state: State): Gen[PrepareCmd] =
    for {
      blockCreated <- genValidBlock(state).map(_.event).map { bc =>
        bc.copy[TestAgreement](
          block = bc.block.copy(
            blockHash = state.maybeBlockHash.getOrElse(bc.block.blockHash)
          )
        )
      }
    } yield {
      PrepareCmd(
        sender = state.leader,
        message = Message.Prepare(
          state.viewNumber,
          blockCreated.block,
          blockCreated.highQC
        )
      )
    }

  /** Replica sends a valid vote for the current phase and prepared block. */
  def genValidVote(state: State): Gen[VoteCmd] =
    for {
      blockHash <- genLazy {
        state.maybeBlockHash.getOrElse(sys.error("No block to vote on."))
      }
      // The leader is expecting votes for the previous phase.
      phase = votingPhaseFor(state.phase).getOrElse(
        sys.error(s"No voting phase for ${state.phase}")
      )
      sender <- Gen.oneOf(state.federation)
      vote = Message.Vote[TestAgreement](
        state.viewNumber,
        phase,
        blockHash,
        signature = mockSigning.sign(
          mockSigningKey(sender),
          phase,
          state.viewNumber,
          blockHash
        )
      )
    } yield VoteCmd(sender, vote)

  /** Leader sends a valid quorum from the collected votes. */
  def genValidQuorum(state: State): Gen[QuorumCmd] =
    for {
      blockHash <- genLazy {
        state.maybeBlockHash.getOrElse(sys.error("No block for quorum."))
      }
      pks <- Gen.pick(state.quorumSize, state.federation)
      // The replicas is expecting the Q.C. for the previous phase.
      phase = votingPhaseFor(state.phase).getOrElse(
        sys.error(s"No voting phase for ${state.phase}")
      )
      qc = QuorumCertificate[TestAgreement, VotingPhase](
        phase,
        state.viewNumber,
        blockHash,
        signature = mockSigning.combine(
          pks.toList.map { pk =>
            mockSigning.sign(
              mockSigningKey(pk),
              phase,
              state.viewNumber,
              blockHash
            )
          }
        )
      )
      q = Message.Quorum(state.viewNumber, qc)
    } yield QuorumCmd(state.leader, q)

  // A positive hash, not the same as Genesis.
  val genHash: Gen[TestAgreement.Hash] =
    arbitrary[Int].map(math.abs(_) + 1)

  /** Timeout. */
  case class NextViewCmd(viewNumber: ViewNumber) extends Command {
    type Result = ProtocolState.Transition[TestAgreement]

    def run(sut: Sut): Result = {
      sut.state.handleNextView(Event.NextView(viewNumber)) match {
        case result @ (next, _) =>
          sut.state = next
          result
      }
    }

    def nextState(state: State): State =
      state.copy(
        viewNumber = ViewNumber(state.viewNumber + 1),
        phase = Phase.Prepare,
        votesFrom = Set.empty,
        // In this model there's not a guaranteed message from the leader to itself.
        newViewsFrom = Set.empty,
        newViewsHighQC = genesisQC,
        maybeBlockHash = None
      )

    def preCondition(state: State): Boolean =
      viewNumber == state.viewNumber

    def postCondition(state: Model, result: Try[Result]): Prop =
      "NextView" |: {
        result match {
          case Failure(exception) =>
            fail(s"unexpected $exception")

          case Success((next, effects)) =>
            val propNewView = effects
              .collectFirst {
                case Effect.SendMessage(
                      recipient,
                      Message.NewView(viewNumber, prepareQC)
                    ) =>
                  "sends the new view to the next leader" |:
                    recipient == next.leader &&
                    viewNumber == state.viewNumber &&
                    prepareQC == next.prepareQC
              }
              .getOrElse(fail("didn't send the new view"))

            val propSchedule = effects
              .collectFirst {
                case Effect.ScheduleNextView(
                      viewNumber,
                      timeout
                    ) =>
                  "schedules the next view" |:
                    viewNumber == next.viewNumber &&
                    timeout == next.timeout
              }
              .getOrElse(fail("didn't schedule the next view"))

            val propNext = "goes to the next phase" |:
              next.phase == Phase.Prepare &&
              next.viewNumber == state.viewNumber + 1 &&
              next.votes.isEmpty &&
              next.newViews.isEmpty

            propNext &&
            propNewView &&
            propSchedule &&
            ("only has the expected effects" |: effects.size == 2)
        }
      }
  }

  /** Common logic of handling a received message */
  sealed trait MessageCmd extends Command {
    type Result = ProtocolState.TransitionAttempt[TestAgreement]

    def sender: TestAgreement.PKey
    def message: Message[TestAgreement]

    override def run(sut: Protocol): Result = {
      val event = Event.MessageReceived(sender, message)
      sut.state.validateMessage(event).flatMap(sut.state.handleMessage).map {
        case result @ (next, _) =>
          sut.state = next
          result
      }
    }
  }

  /** NewView from a replicas to the leader. */
  case class NewViewCmd(
      sender: TestAgreement.PKey,
      message: Message.NewView[TestAgreement]
  ) extends MessageCmd {
    override def nextState(state: State): State =
      state.copy(
        newViewsFrom = state.newViewsFrom + sender,
        newViewsHighQC =
          if (message.prepareQC.viewNumber > state.newViewsHighQC.viewNumber)
            message.prepareQC
          else state.newViewsHighQC
      )

    override def preCondition(state: State): Boolean =
      state.isLeader && state.viewNumber == message.viewNumber + 1

    override def postCondition(
        state: Model,
        result: Try[Result]
    ): Prop = {
      val nextS = nextState(state)
      "NewView" |: {
        if (
          state.phase == Phase.Prepare &&
          state.newViewsFrom.size != state.quorumSize &&
          nextS.newViewsFrom.size == state.quorumSize
        ) {
          result match {
            case Success(Right((next, effects))) =>
              val newViewsMax = nextS.newViewsHighQC.viewNumber
              val highestView = effects.headOption match {
                case Some(Effect.CreateBlock(_, highQC)) =>
                  highQC.viewNumber.toInt
                case _ => -1
              }

              "n-f collected" |: all(
                s"stays in the phase (${state.phase} -> ${next.phase})" |: next.phase == state.phase,
                "records newView" |: next.newViews.size == state.quorumSize,
                "creates a block and nothing else" |: effects.size == 1 &&
                  effects.head.isInstanceOf[Effect.CreateBlock[_]],
                s"selects the highest QC: $highestView ?= $newViewsMax" |: highestView == newViewsMax
              )
            case err =>
              fail(s"unexpected $err")
          }
        } else {
          result match {
            case Success(Right((next, effects))) =>
              "n-f not collected" |: all(
                s"stays in the same phase (${state.phase} -> ${next.phase})" |: next.phase == state.phase,
                "doesn't create more effects" |: effects.isEmpty
              )
            case err =>
              fail(s"unexpected $err")
          }
        }
      }
    }
  }

  /** The leader handed the block created by the host system. */
  case class BlockCreatedCmd(event: Event.BlockCreated[TestAgreement])
      extends Command {
    type Result = ProtocolState.Transition[TestAgreement]

    override def run(sut: Protocol): Result = {
      sut.state.handleBlockCreated(event) match {
        case result @ (next, _) =>
          sut.state = next
          result
      }
    }

    override def nextState(state: State): State =
      state.copy(
        maybeBlockHash = Some(event.block.blockHash)
      )

    override def preCondition(state: State): Boolean =
      event.viewNumber == state.viewNumber

    override def postCondition(
        state: State,
        result: Try[Result]
    ): Prop = {
      "BlockCreated" |: {
        result match {
          case Success((next, effects)) =>
            all(
              "stay in Prepare" |: next.phase == Phase.Prepare,
              "broadcast to all" |: effects.size == state.federation.size,
              all(
                effects.map {
                  case Effect.SendMessage(_, m: Message.Prepare[_]) =>
                    all(
                      "send prepared block" |: m.block == event.block,
                      "send highQC" |: m.highQC == event.highQC
                    )
                  case other =>
                    fail(s"expected Prepare message: $other")
                }: _*
              )
            )
          case Failure(ex) =>
            fail(s"failed with $ex")
        }
      }
    }
  }

  /** Prepare from leader to a replica. */
  case class PrepareCmd(
      sender: TestAgreement.PKey,
      message: Message.Prepare[TestAgreement]
  ) extends MessageCmd {
    override def nextState(state: State): State = {
      state.copy(
        phase = Phase.PreCommit,
        maybeBlockHash = Some(message.block.blockHash)
      )
    }

    override def preCondition(state: State): Boolean = {
      message.viewNumber == state.viewNumber &&
      state.phase == Phase.Prepare &&
      (state.isLeader && state.maybeBlockHash.isDefined ||
        !state.isLeader && state.maybeBlockHash.isEmpty)
    }

    override def postCondition(
        state: Model,
        result: Try[Result]
    ): Prop = {
      "Prepare" |: {
        result match {
          case Success(Right((next, effects))) =>
            all(
              "move to PreCommit" |: next.phase == Phase.PreCommit,
              "cast a vote" |: effects.size == 1,
              effects.head match {
                case Effect.SendMessage(
                      recipient,
                      Message.Vote(_, phase, blockHash, _)
                    ) =>
                  all(
                    "vote Prepare" |: phase == Phase.Prepare,
                    "send to leader" |: recipient == state.leader,
                    "vote on block" |: blockHash == message.block.blockHash
                  )
                case other =>
                  fail(s"unexpected effect $other")
              }
            )
          case other =>
            fail(s"unexpected result $other")
        }
      }
    }
  }

  /** A Vote from a replica to the leader. */
  case class VoteCmd(
      sender: TestAgreement.PKey,
      message: Message.Vote[TestAgreement]
  ) extends MessageCmd {
    override def nextState(state: State): State =
      state.copy(
        votesFrom = state.votesFrom + sender
      )

    override def preCondition(state: State): Boolean =
      state.isLeader &&
        state.viewNumber == message.viewNumber &&
        votingPhaseFor(state.phase).contains(message.phase) &&
        state.maybeBlockHash.contains(message.blockHash)

    override def postCondition(state: Model, result: Try[Result]): Prop = {
      "Vote" |: {
        result match {
          case Success(Right((next, effects))) =>
            val nextS = nextState(state)
            val maybeBroadcast =
              if (
                state.votesFrom.size < state.quorumSize &&
                nextS.votesFrom.size == state.quorumSize
              ) {
                "n - f collected" |: all(
                  "broadcast to all" |: effects.size == state.federation.size,
                  "all messages are quorums" |: all(
                    effects.map {
                      case Effect.SendMessage(_, Message.Quorum(_, qc)) =>
                        all(
                          "quorum is about the current phase" |: qc.phase == message.phase,
                          "quorum is about the block" |: qc.blockHash == message.blockHash
                        )
                      case other =>
                        fail(s"unexpected effect $other")
                    }: _*
                  )
                )
              } else {
                "not n - f" |: "not broadcast" |: effects.isEmpty
              }

            all(
              "stay in the same phase" |: next.phase == state.phase,
              maybeBroadcast
            )

          case other =>
            fail(s"unexpected result $other")
        }
      }
    }
  }

  /** A Quorum from the leader to a replica. */
  case class QuorumCmd(
      sender: TestAgreement.PKey,
      message: Message.Quorum[TestAgreement]
  ) extends MessageCmd {
    override def nextState(state: State): State =
      state.copy(
        viewNumber =
          if (state.phase == Phase.Decide) state.viewNumber.next
          else state.viewNumber,
        phase = state.phase match {
          case Phase.Prepare   => Phase.PreCommit
          case Phase.PreCommit => Phase.Commit
          case Phase.Commit    => Phase.Decide
          case Phase.Decide    => Phase.Prepare
        },
        votesFrom = Set.empty,
        newViewsFrom = Set.empty,
        maybeBlockHash =
          if (state.phase == Phase.Decide) None else state.maybeBlockHash,
        prepareQCs =
          if (message.quorumCertificate.phase == Phase.Prepare)
            message.quorumCertificate.coerce[Phase.Prepare] :: state.prepareQCs
          else state.prepareQCs,
        newViewsHighQC =
          if (state.phase == Phase.Decide) genesisQC else state.newViewsHighQC
      )

    override def preCondition(state: State): Boolean =
      state.viewNumber == message.viewNumber &&
        votingPhaseFor(state.phase).contains(message.quorumCertificate.phase) &&
        state.maybeBlockHash.contains(message.quorumCertificate.blockHash)

    override def postCondition(
        state: Model,
        result: Try[Result]
    ): Prop = {
      "Quorum" |: {
        result match {
          case Success(Right((next, effects))) =>
            val nextS = nextState(state)
            all(
              "moves to the next state" |: next.phase == nextS.phase,
              "votes for the next phase" |: (state.phase == Phase.Decide ||
                effects
                  .collectFirst {
                    case Effect.SendMessage(_, Message.Vote(_, phase, _, _)) =>
                      phase == state.phase
                  }
                  .getOrElse(false)),
              "makes a decision" |: (state.phase != Phase.Decide ||
                all(
                  "executes the block" |: effects.collectFirst {
                    case _: Effect.ExecuteBlocks[_] =>
                  }.isDefined,
                  "remembers the executed block" |:
                    next.lastExecutedBlockHash == message.quorumCertificate.blockHash
                )),
              "saves the prepared block" |: (state.phase != Phase.PreCommit ||
                effects.collectFirst { case _: Effect.SaveBlock[_] =>
                }.isDefined)
            )

          case other =>
            fail(s"unexpected result $other")
        }
      }
    }
  }

  /** Check that a deliberately invalidated command returns a protocol error. */
  case class InvalidCmd(label: String, cmd: MessageCmd, isEarly: Boolean)
      extends Command {
    type Result = (Boolean, ProtocolState.TransitionAttempt[TestAgreement])

    // The underlying command should return a `Left`,
    // which means it shouldn't update the state.
    override def run(sut: Protocol): Result = {
      val event             = Event.MessageReceived(cmd.sender, cmd.message)
      val isStaticallyValid = sut.state.validateMessage(event).isRight
      isStaticallyValid -> cmd.run(sut)
    }

    // The model state validation is not as sophisticated,
    // but because we know this is invalid, we know
    // it should not cause any change in state.
    override def nextState(state: State): State =
      state

    // The invalidation should be strong enough that it doesn't
    // become valid during shrinking.
    override def preCondition(state: State): Boolean =
      true

    override def postCondition(
        state: State,
        result: Try[Result]
    ): Prop =
      s"Invalid $label" |: {
        result match {
          case Success((isStaticallyValid, Left(error))) =>
            // Ensure that some errors are marked as TooEarly.
            "is early" |:
              isEarly && isStaticallyValid && error
                .isInstanceOf[ProtocolError.TooEarly[_]] ||
              !isStaticallyValid ||
              !isEarly

          case other =>
            fail(s"unexpected result $other")
        }
      }

  }
}
