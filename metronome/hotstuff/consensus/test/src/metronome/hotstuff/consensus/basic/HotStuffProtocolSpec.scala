package metronome.hotstuff.consensus.basic

import metronome.crypto.{GroupSignature, PartialSignature}
import metronome.hotstuff.consensus.{ViewNumber, Federation}
import org.scalacheck.commands.Commands
import org.scalacheck.{Properties, Gen, Prop, Test, Arbitrary}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.forAll
import scala.annotation.nowarn
import scala.concurrent.duration._

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

  // Mock signatures.
  implicit val signing: Signing[TestAgreement] = new Signing[TestAgreement] {

    // Going to use publicKey == -1 * signingKey.
    // signature = hash * signingKey
    // publicKey = -1 * signature / hash

    private def hash(
        phase: VotingPhase,
        viewNumber: ViewNumber,
        blockHash: TestAgreement#Hash
    ): TestAgreement#Hash =
      (phase, viewNumber, blockHash).hashCode

    override def sign(
        signingKey: TestAgreement#SKey,
        phase: VotingPhase,
        viewNumber: ViewNumber,
        blockHash: TestAgreement#Hash
    ): Signing.PartialSig[TestAgreement] = {
      val h = hash(phase, viewNumber, blockHash)
      PartialSignature(h * signingKey)
    }

    override def combine(
        signatures: Seq[Signing.PartialSig[TestAgreement]]
    ): Signing.GroupSig[TestAgreement] =
      GroupSignature(signatures.map(_.sig))

    override def validate(
        publicKey: TestAgreement#PKey,
        signature: Signing.PartialSig[TestAgreement],
        phase: VotingPhase,
        viewNumber: ViewNumber,
        blockHash: TestAgreement#Hash
    ): Boolean = {
      val h = hash(phase, viewNumber, blockHash)
      publicKey == -1 * signature.sig / h
    }

    override def validate(
        federation: Federation[TestAgreement#PKey],
        signature: Signing.GroupSig[TestAgreement],
        phase: VotingPhase,
        viewNumber: ViewNumber,
        blockHash: TestAgreement#Hash
    ): Boolean = {
      if (
        phase == genesisQC.phase &&
        viewNumber == genesisQC.viewNumber &&
        blockHash == genesisQC.blockHash
      ) {
        signature.sig.isEmpty
      } else {
        val h = hash(phase, viewNumber, blockHash)
        signature.sig.size == federation.size - federation.maxFaulty &&
        signature.sig.forall { sig =>
          federation.publicKeys.exists { publicKey =>
            publicKey == sig - h
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
      voteCount: Int,
      newViewCount: Int
  ) {
    def publicKey = federation(ownIndex)

    // Using a signing key that works with the mock validation.
    def signingKey = -1 * publicKey

    def isLeader = viewNumber % n == ownIndex
  }

  type Sut   = ProtocolState[TestAgreement]
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
      state.newViewCount == 0 &&
      state.voteCount == 0

  override def newSut(state: State): Sut =
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

  override def destroySut(sut: Sut): Unit = ()

  override def genInitialState: Gen[State] =
    for {
      // Pick the max Byzantine nodes first, then size the federation based on that.
      f <- Gen.choose(0, 3)
      n = 3 * f + 1

      ownIndex <- Gen.choose(0, n - 1)

      // Create unique keys.
      publicKeys <- Gen
        .listOfN(n, arbitrary[Int])
        .map(_.toSet)
        .suchThat(_.size == n)

    } yield Model(
      n,
      f,
      viewNumber = 1,
      phase = Phase.Prepare,
      federation = publicKeys.toVector,
      ownIndex = ownIndex,
      voteCount = 0,
      newViewCount = 0
    )

  override def genCommand(state: State): Gen[Command] = ???

}
