package metronome.hotstuff.consensus.basic

import metronome.crypto.{GroupSignature, PartialSignature}
import metronome.hotstuff.consensus.{ViewNumber, Federation}
import org.scalacheck.commands.Commands
import org.scalacheck.{Properties, Gen, Prop, Test, Arbitrary}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.forAll
import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.util.Try
import scala.util.Failure
import scala.util.Success

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
      viewNumber: Int,
      phase: Phase,
      federation: Vector[TestAgreement.PKey],
      ownIndex: Int,
      votesFrom: Set[TestAgreement.PKey],
      newViewsFrom: Set[TestAgreement.PKey]
  ) {
    def publicKey = federation(ownIndex)

    // Using a signing key that works with the mock validation.
    def signingKey = mockSigningKey(publicKey)

    def isLeader = viewNumber % n == ownIndex

    def `n - f` = n - f
  }

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
      viewNumber = 1,
      phase = Phase.Prepare,
      federation = publicKeys.toVector,
      ownIndex = ownIndex,
      votesFrom = Set.empty,
      newViewsFrom = Set.empty
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

  def genValid(state: State): Gen[Command] =
    if (state.isLeader) {
      //Gen.oneOf(
      genValidNewView(state) //,
      //genValidVote(state)
      //)
    } else {
      genTimeout(state)
    }

  def genValidNewView(state: State): Gen[Command] =
    for {
      s  <- Gen.oneOf(state.federation)
      qc <- genPrepareQC(state)
      m = Message.NewView(ViewNumber(state.viewNumber - 1), qc)
    } yield NewViewCmd(s, m)

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

  case class NextViewCmd(viewNumber: Int) extends Command {
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
        viewNumber = state.viewNumber + 1,
        phase = Phase.Prepare,
        votesFrom = Set.empty,
        newViewsFrom = Set.empty
      )

    def preCondition(state: State): Boolean =
      viewNumber == state.viewNumber

    def postCondition(state: Model, result: Try[Result]): Prop =
      result match {
        case Failure(exception) => false
        case Success((next, effects)) =>
          val propNewView = effects.collectFirst {
            case Effect.SendMessage(
                  recipient,
                  Message.NewView(viewNumber, prepareQC)
                ) =>
              recipient == next.leader &&
                viewNumber == state.viewNumber &&
                prepareQC == next.prepareQC
          }
          val propSchedule = effects.collectFirst {
            case Effect.ScheduleNextView(
                  viewNumber,
                  timeout
                ) =>
              viewNumber == next.viewNumber &&
                timeout == next.timeout
          }
          val propNext =
            next.phase == Phase.Prepare &&
              next.viewNumber == state.viewNumber + 1 &&
              next.votes.isEmpty &&
              next.newViews.isEmpty

          propNext &&
          effects.size == 2 &&
          propNewView == Some(true) &&
          propSchedule == Some(true)
      }
  }

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

  case class NewViewCmd(
      sender: TestAgreement.PKey,
      message: Message.NewView[TestAgreement]
  ) extends MessageCmd {
    override def nextState(state: State): State =
      if (state.phase == Phase.Prepare) {
        state.copy(
          newViewsFrom = state.newViewsFrom + sender
        )
      } else state

    override def preCondition(state: State): Boolean =
      state.isLeader && state.viewNumber == message.viewNumber + 1

    override def postCondition(
        state: Model,
        result: Try[Result]
    ): Prop =
      if (
        state.phase == Phase.Prepare && (state.newViewsFrom + sender).size == state.`n - f`
      ) {
        result match {
          case Success(Right((next, effects))) =>
            next.phase == state.phase &&
              next.newViews.size == state.`n - f` &&
              effects.size == 1 &&
              effects.head.isInstanceOf[Effect.CreateBlock[_]]
          case _ =>
            false
        }
      } else {
        result match {
          case Success(Right((next, effects))) =>
            next.phase == state.phase && effects.isEmpty
          case _ =>
            false
        }
      }

  }

}
