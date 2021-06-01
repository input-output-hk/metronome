package io.iohk.metronome.hotstuff.service.storage

import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.{
  QuorumCertificate,
  Phase,
  Agreement
}
import scala.annotation.nowarn
import io.iohk.metronome.crypto.GroupSignature
import io.iohk.metronome.storage.{KVStore, KVStoreRead, KVStoreState}
import org.scalacheck.{Gen, Prop, Properties}
import org.scalacheck.commands.Commands
import org.scalacheck.Arbitrary.arbitrary
import scala.util.Try
import scodec.bits.BitVector
import scodec.Codec
import scala.util.Success
import io.iohk.metronome.hotstuff.consensus.basic.VotingPhase

object ViewStateStorageProps extends Properties("ViewStateStorage") {
  property("commands") = ViewStateStorageCommands.property()
}

object ViewStateStorageCommands extends Commands {
  object TestAgreement extends Agreement {
    type Block = Nothing
    type Hash  = String
    type PSig  = Unit
    type GSig  = List[String]
    type PKey  = Nothing
    type SKey  = Nothing
  }
  type TestAgreement = TestAgreement.type

  type Namespace = String

  object TestKVStoreState extends KVStoreState[Namespace]

  type TestViewStateStorage = ViewStateStorage[Namespace, TestAgreement]

  class StorageWrapper(
      viewStateStorage: TestViewStateStorage,
      private var store: TestKVStoreState.Store
  ) {
    def getStore = store

    def write(
        f: TestViewStateStorage => KVStore[Namespace, Unit]
    ): Unit = {
      store = TestKVStoreState.compile(f(viewStateStorage)).runS(store).value
    }

    def read[A](
        f: TestViewStateStorage => KVStoreRead[Namespace, A]
    ): A = {
      val b = scodec.bits.ByteVector.empty
      TestKVStoreState.compile(f(viewStateStorage)).run(store)
    }
  }

  type State = ViewStateStorage.Bundle[TestAgreement]
  type Sut   = StorageWrapper

  val genesisState = ViewStateStorage.Bundle
    .fromGenesisQC[TestAgreement] {
      QuorumCertificate[TestAgreement, Phase.Prepare](
        Phase.Prepare,
        ViewNumber(1),
        "",
        GroupSignature(Nil)
      )
    }

  /** The in-memory KVStoreState doesn't invoke the codecs. */
  implicit def neverUsedCodec[T] =
    Codec[T](
      (_: T) => sys.error("Didn't expect to encode."),
      (_: BitVector) => sys.error("Didn't expect to decode.")
    )

  @nowarn
  override def canCreateNewSut(
      newState: State,
      initSuts: Traversable[State],
      runningSuts: Traversable[Sut]
  ): Boolean = true

  override def initialPreCondition(state: State): Boolean =
    state == genesisState

  override def newSut(state: State): Sut = {
    val init = TestKVStoreState.compile(
      ViewStateStorage[Namespace, TestAgreement]("test-namespace", state)
    )
    val (store, storage) = init.run(Map.empty).value
    new StorageWrapper(storage, store)
  }

  override def destroySut(sut: Sut): Unit = ()

  override def genInitialState: Gen[State] = Gen.const(genesisState)

  override def genCommand(state: State): Gen[Command] =
    Gen.oneOf(
      genSetViewNumber(state),
      genSetQuorumCertificate(state),
      genSetLastExecutedBlockHash(state),
      genGetBundle
    )

  def genSetViewNumber(state: State) =
    for {
      d <- Gen.posNum[Long]
      vn = ViewNumber(state.viewNumber + d)
    } yield SetViewNumberCommand(vn)

  def genSetQuorumCertificate(state: State) =
    for {
      p <- Gen.oneOf(Phase.Prepare, Phase.PreCommit, Phase.Commit)
      h <- arbitrary[TestAgreement.Hash]
      s <- arbitrary[TestAgreement.GSig]
      qc = QuorumCertificate[TestAgreement, VotingPhase](
        p,
        state.viewNumber,
        h,
        GroupSignature(s)
      )
    } yield SetQuorumCertificateCommand(qc)

  def genSetLastExecutedBlockHash(state: State) =
    for {
      h <- Gen.oneOf(
        state.prepareQC.blockHash,
        state.lockedQC.blockHash,
        state.commitQC.blockHash
      )
    } yield SetLastExecutedBlockHashCommand(h)

  def genSetRootBlockHash(state: State) =
    for {
      h <- Gen.oneOf(
        state.prepareQC.blockHash,
        state.lockedQC.blockHash,
        state.commitQC.blockHash
      )
    } yield SetRootBlockHashCommand(h)

  val genGetBundle = Gen.const(GetBundleCommand)

  case class SetViewNumberCommand(viewNumber: ViewNumber) extends UnitCommand {
    override def run(sut: Sut): Result =
      sut.write(_.setViewNumber(viewNumber))
    override def nextState(state: State): State =
      state.copy(viewNumber = viewNumber)
    override def preCondition(state: State): Boolean =
      state.viewNumber < viewNumber
    override def postCondition(state: State, success: Boolean): Prop = success
  }

  case class SetQuorumCertificateCommand(
      qc: QuorumCertificate[TestAgreement, VotingPhase]
  ) extends UnitCommand {
    override def run(sut: Sut): Result =
      sut.write(_.setQuorumCertificate(qc))

    override def nextState(state: State): State =
      qc.phase match {
        case Phase.Prepare =>
          state.copy(prepareQC = qc.coerce[Phase.Prepare])
        case Phase.PreCommit =>
          state.copy(lockedQC = qc.coerce[Phase.PreCommit])
        case Phase.Commit =>
          state.copy(commitQC = qc.coerce[Phase.Commit])
      }

    override def preCondition(state: State): Boolean =
      state.viewNumber <= qc.viewNumber

    override def postCondition(state: State, success: Boolean): Prop = success
  }

  case class SetLastExecutedBlockHashCommand(blockHash: TestAgreement.Hash)
      extends UnitCommand {
    override def run(sut: Sut): Result =
      sut.write(_.setLastExecutedBlockHash(blockHash))

    override def nextState(state: State): State =
      state.copy(lastExecutedBlockHash = blockHash)

    override def preCondition(state: State): Boolean =
      Set(state.prepareQC, state.lockedQC, state.commitQC)
        .map(_.blockHash)
        .contains(blockHash)

    override def postCondition(state: State, success: Boolean): Prop = success
  }

  case class SetRootBlockHashCommand(blockHash: TestAgreement.Hash)
      extends UnitCommand {
    override def run(sut: Sut): Result =
      sut.write(_.setRootBlockHash(blockHash))

    override def nextState(state: State): State =
      state.copy(rootBlockHash = blockHash)

    override def preCondition(state: State): Boolean =
      Set(state.prepareQC, state.lockedQC, state.commitQC)
        .map(_.blockHash)
        .contains(blockHash)

    override def postCondition(state: State, success: Boolean): Prop = success
  }

  case object GetBundleCommand extends Command {
    type Result = ViewStateStorage.Bundle[TestAgreement]

    override def run(sut: Sut): Result               = sut.read(_.getBundle)
    override def nextState(state: State): State      = state
    override def preCondition(state: State): Boolean = true
    override def postCondition(state: State, result: Try[Result]): Prop =
      result == Success(state)
  }
}
