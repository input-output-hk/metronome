package io.iohk.metronome.examples.robot.service

import cats.implicits._
import cats.effect.Sync
import cats.data.{NonEmptyList, NonEmptyVector}
import io.iohk.metronome.crypto.ECPublicKey
import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.hotstuff.service.ApplicationService
import io.iohk.metronome.examples.robot.RobotAgreement
import io.iohk.metronome.examples.robot.models.{RobotBlock, Robot}
import io.iohk.metronome.examples.robot.service.messages.RobotMessage
import io.iohk.metronome.hotstuff.consensus.basic.QuorumCertificate
import io.iohk.metronome.hotstuff.service.Network
import io.iohk.metronome.hotstuff.service.storage.{
  BlockStorage,
  ViewStateStorage
}
import io.iohk.metronome.storage.{KVStoreRunner, KVRingBuffer}

class RobotService[F[_]: Sync, N](
    maxRow: Int,
    maxCol: Int,
    network: Network[F, RobotAgreement, RobotMessage],
    blockStorage: BlockStorage[N, RobotAgreement],
    viewStateStorage: ViewStateStorage[N, RobotAgreement],
    stateStorage: KVRingBuffer[N, Hash, Robot.State]
)(implicit storeRunner: KVStoreRunner[F, N])
    extends ApplicationService[F, RobotAgreement] {

  def createBlock(
      highQC: QuorumCertificate[RobotAgreement]
  ): F[Option[RobotBlock]] = {
    // Retrieve the blocks that we need to build on.
          val query = for {
            lastExecutedBlockHash <- viewStateStorage.getLastExecutedBlockHash
            pendingBlockPath <- blockStorage.getPathFromAncestor(lastExecutedBlockHash, highQC.blockHash)
            lastExecutedState <- stateStorage.get()
          }
  }

  def validateBlock(block: RobotBlock): F[Boolean] = ???

  def executeBlock(
      block: RobotBlock,
      commitQC: QuorumCertificate[RobotAgreement],
      commitPath: NonEmptyList[RobotAgreement.Hash]
  ): F[Unit] = ???

  def syncState(
      sources: NonEmptyVector[ECPublicKey],
      block: RobotBlock
  ): F[Unit] = ???

}
