package metronome.hotstuff.consensus.basic

import metronome.crypto.{GroupSignature, PartialSignature}
import metronome.hotstuff.consensus.{ViewNumber, Federation}
import org.scalacheck.commands.Commands
import org.scalacheck.{Properties, Gen, Prop, Test, Arbitrary}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.{forAll, propBoolean, all, falsified}
import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import metronome.hotstuff.consensus.basic.Effect.CreateBlock

object HotStuffProtocolSpec extends Properties("Basic HotStuff") {

  property("state") = HotStuffProtocolCommands.property()

}

object HotStuffProtocolCommands extends Commands {

  case class TestBlock(blockHash: Int, parentBlockHash: Int, command: String)

  object TestAgreement extends Agreement {
    type Block = TestBlock
    type Hash  = Int
    type PSig  = Long
    type GSig  = Seq[Long]
    type PKey  = Int
    type SKey  = Int
  }
  type TestAgreement = TestAgreement.type

  val genesisQC = QuorumCertificate[TestAgreement](
    phase = Phase.Prepare,
    viewNumber = ViewNumber(0),
    blockHash = 0,
    signature = GroupSignature(Nil)
  )

  implicit val block: Block[TestAgreement] = new Block[TestAgreement] {
    override def blockHash(b: TestBlock)       = b.blockHash
    override def parentBlockHash(b: TestBlock) = b.parentBlockHash
  }

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
          phase: VotingPhase,
          viewNumber: ViewNumber,
          blockHash: TestAgreement.Hash
      ): Boolean =
        phase == genesisQC.phase &&
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
        if (isGenesis(phase, viewNumber, blockHash)) {
          signature.sig.isEmpty
        } else {
          val h = hash(phase, viewNumber, blockHash)

          signature.sig.size == federation.size - federation.maxFaulty &&
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
      newViewsHighQC: QuorumCertificate[TestAgreement],
      maybeBlockCreated: Option[Event.BlockCreated[TestAgreement]]
      // TODO: Keep a list of previous prepareQC items and pick
      // from them for NewView, rather than generate an arbitrary one.
  ) {
    def publicKey = federation(ownIndex)

    // Using a signing key that works with the mock validation.
    def signingKey = mockSigningKey(publicKey)

    def isLeader = viewNumber % n == ownIndex

    def `n - f` = n - f
  }

  // Keep a variable state in our System Under Test.
  class Protocol(var state: ProtocolState[TestAgreement])

  type Sut   = Protocol
  type State = Model

  private def fail(msg: String) = msg |: falsified

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

  override def newSut(state: State): Sut =
    new Protocol(
      ProtocolState[TestAgreement](
        viewNumber = ViewNumber(state.viewNumber),
        phase = state.phase,
        publicKey = state.publicKey,
        signingKey = state.signingKey,
        federation = Federation(state.federation),
        prepareQC = genesisQC,
        lockedQC = genesisQC,
        lastExecutedBlockHash = genesisQC.blockHash,
        preparedBlockHash = genesisQC.blockHash,
        timeout = 10.seconds,
        votes = Set.empty,
        newViews = Map.empty
      )
    )

  override def destroySut(sut: Sut): Unit = ()

  override def genInitialState: Gen[State] =
    for {
      // Pick the max Byzantine nodes first, then size the federation based on that.
      f <- Gen.choose(0, 3)
      n = 3 * f + 1

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
      maybeBlockCreated = None
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
      // 3 -> genInvalid,
      // 2 -> genUnexpected(state),
      1 -> genTimeout(state)
    )

  def genTimeout(state: State): Gen[Command] =
    Gen.const(NextViewCmd(state.viewNumber))

  def genValid(state: State): Gen[Command] = {
    val usables: List[Gen[Command]] =
      List(
        genValidNewView(state) -> state.isLeader,
        genValidBlock(state)   -> state.isLeader,
        genValidPrepare(state) -> true,
        genValidVote(state)    -> state.isLeader,
        genValidQuorum(state)  -> true
      ).collect {
        case (gen, usable) if usable => gen
      }

    usables match {
      case Nil                => genTimeout(state)
      case one :: Nil         => one
      case one :: two :: rest => Gen.oneOf(one, two, rest: _*)
    }
  }

  /** Replica sends a new view with an arbitrary prepare QC. */
  def genValidNewView(state: State): Gen[Command] =
    for {
      s  <- Gen.oneOf(state.federation)
      qc <- genPrepareQC(state)
      m = Message.NewView(ViewNumber(state.viewNumber - 1), qc)
    } yield NewViewCmd(s, m)

  /** Leader creates a valid block on top of the saved High Q.C. */
  def genValidBlock(state: State): Gen[Command] =
    for {
      c <- arbitrary[String]
      h <- genHash
      p = state.newViewsHighQC.blockHash
      b = TestBlock(h, p, c)
      e = Event
        .BlockCreated[TestAgreement](state.viewNumber, b, state.newViewsHighQC)
    } yield BlockCreatedCmd(e)

  /** Leader sends a valid Prepare command with the generated block. */
  def genValidPrepare(state: State): Gen[Command] = genTimeout(state)

  /** Replica sends a valid vote for the current phase and prepared block. */
  def genValidVote(state: State): Gen[Command] = genTimeout(state)

  /** Leader sends a valid quorum from the collected votes. */
  def genValidQuorum(state: State): Gen[Command] = genTimeout(state)

  // A positive hash, not the same as Genesis.
  val genHash: Gen[TestAgreement.Hash] =
    arbitrary[Int].map(math.abs(_) + 1)

  def genPrepareQC(state: State): Gen[QuorumCertificate[TestAgreement]] =
    for {
      publicKeys <- Gen.pick(state.`n - f`, state.federation)
      phase = Phase.Prepare
      viewNumber <- Gen
        .choose(1, 5)
        .map(d => ViewNumber(math.max(0, state.viewNumber - d)))
      blockHash <- genHash
      partials = publicKeys.toList.map { pk =>
        val sk = mockSigningKey(pk)
        mockSigning.sign(sk, phase, viewNumber, blockHash)
      }
      signature = mockSigning.combine(partials)
    } yield QuorumCertificate[TestAgreement](
      phase,
      viewNumber,
      blockHash,
      signature
    )

  // def genInvalid(state: State): Gen[Command] =
  //   Gen.oneOf(
  //     genNotFromFederation(state),
  //     genNotFromLeader(state),
  //     genNotToLeader(state),
  //     genInvalidVote(state),
  //     genInvalidQuorumCertificate(state),
  //     genUnsafeExtension(state)
  //   )

  //def genNotFromFederation(state: State): Gen[Command] =

  type Transition = ProtocolState.Transition[TestAgreement]

  /** Timeout. */
  case class NextViewCmd(viewNumber: ViewNumber) extends Command {
    type Result = Transition

    def run(sut: Sut): Result = {
      sut.state.handleNextView(Event.NextView(ViewNumber(viewNumber))) match {
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
        newViewsHighQC = genesisQC
      )

    def preCondition(state: State): Boolean =
      viewNumber == state.viewNumber

    def postCondition(state: Model, result: Try[Result]): Prop =
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

  /** Common logic of handling a received message */
  trait MessageCmd extends Command {
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
      if (state.phase == Phase.Prepare) {
        state.copy(
          newViewsFrom = state.newViewsFrom + sender,
          newViewsHighQC =
            if (message.prepareQC.viewNumber > state.newViewsHighQC.viewNumber)
              message.prepareQC
            else state.newViewsHighQC
        )
      } else state

    override def preCondition(state: State): Boolean =
      state.isLeader && state.viewNumber == message.viewNumber + 1

    override def postCondition(
        state: Model,
        result: Try[Result]
    ): Prop = {
      val nextS = nextState(state)
      if (
        state.phase == Phase.Prepare && nextS.newViewsFrom.size == state.`n - f`
      ) {
        result match {
          case Success(Right((next, effects))) =>
            val newViewsMax = nextS.newViewsHighQC.viewNumber
            val highestView = effects.headOption match {
              case Some(CreateBlock(_, highQC)) => highQC.viewNumber.toInt
              case _                            => 0
            }

            all(
              "stays in the phase" |: next.phase == state.phase,
              "records newView" |: next.newViews.size == state.`n - f`,
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
            "stays in the phase and doesn't create more blocks" |:
              next.phase == state.phase && effects.isEmpty
          case err =>
            fail(s"unexpected $err")
        }
      }
    }
  }

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
        maybeBlockCreated = Some(event)
      )

    override def preCondition(state: State): Boolean =
      event.viewNumber == state.viewNumber

    override def postCondition(
        state: State,
        result: Try[Result]
    ): Prop = result match {
      case Success((_, effects)) =>
        "broadcast Prepare" |: effects.size == state.federation.size &&
          effects.forall {
            case Effect.SendMessage(_, _: Message.Prepare[_]) => true
            case _                                            => false
          }
      case Failure(ex) =>
        fail(s"failed to broadcast Prepare: $ex")
    }

  }
}
