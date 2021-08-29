package io.iohk.metronome.checkpointing.interpreter.messages

import io.iohk.metronome.checkpointing.models.ArbitraryInstances._
import io.iohk.metronome.checkpointing.models.{Transaction, Ledger, Block}
import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary
import java.util.UUID
import io.iohk.metronome.checkpointing.models.CheckpointCertificate

object ArbitraryInstances {
  import InterpreterMessage._

  implicit val arbNewProposerBlockRequest: Arbitrary[NewProposerBlockRequest] =
    Arbitrary {
      for {
        requestId     <- arbitrary[UUID]
        proposerBlock <- arbitrary[Transaction.ProposerBlock]
      } yield NewProposerBlockRequest(requestId, proposerBlock)
    }

  implicit val arbNewCheckpointCandidateRequest
      : Arbitrary[NewCheckpointCandidateRequest] = Arbitrary {
    for {
      requestId <- arbitrary[UUID]
    } yield NewCheckpointCandidateRequest(requestId)
  }

  implicit val arbCreateBlockBodyRequest: Arbitrary[CreateBlockBodyRequest] =
    Arbitrary {
      for {
        requestId <- arbitrary[UUID]
        ledger    <- arbitrary[Ledger]
        mempool   <- arbitrary[List[Transaction.ProposerBlock]]
      } yield CreateBlockBodyRequest(requestId, ledger, mempool)
    }

  implicit val arbCreateBlockBodyResponse: Arbitrary[CreateBlockBodyResponse] =
    Arbitrary {
      for {
        requestId <- arbitrary[UUID]
        block     <- arbitrary[Block]
        mempool   <- arbitrary[Set[Transaction.ProposerBlock]]
      } yield CreateBlockBodyResponse(requestId, block.body, mempool)
    }

  implicit val arbValidateBlockBodyRequest
      : Arbitrary[ValidateBlockBodyRequest] = Arbitrary {
    for {
      requestId <- arbitrary[UUID]
      block     <- arbitrary[Block]
      ledger    <- arbitrary[Ledger]
    } yield ValidateBlockBodyRequest(requestId, block.body, ledger)
  }

  implicit val arbValidateBlockBodyResponse
      : Arbitrary[ValidateBlockBodyResponse] = Arbitrary {
    for {
      requestId <- arbitrary[UUID]
      isValid   <- arbitrary[Boolean]
    } yield ValidateBlockBodyResponse(requestId, isValid)
  }

  implicit val arbNewCheckpointCertificateRequest
      : Arbitrary[NewCheckpointCertificateRequest] = Arbitrary {
    for {
      requestId             <- arbitrary[UUID]
      checkpointCertificate <- arbitrary[CheckpointCertificate]
    } yield NewCheckpointCertificateRequest(requestId, checkpointCertificate)
  }

  implicit val arbValidExtensionRequest: Arbitrary[ValidExtensionRequest] =
    Arbitrary {
      for {
        requestId   <- arbitrary[UUID]
        blockHeader <- arbitrary[Block.Header]
      } yield ValidExtensionRequest(requestId, blockHeader)
    }

  implicit val arbValidExtensionResponse: Arbitrary[ValidExtensionResponse] =
    Arbitrary {
      for {
        requestId  <- arbitrary[UUID]
        isExtended <- arbitrary[Boolean]
      } yield ValidExtensionResponse(requestId, isExtended)
    }

  implicit val arbAncestryRequest: Arbitrary[AncestryRequest] =
    Arbitrary {
      for {
        requestId           <- arbitrary[UUID]
        targetBlockInfo     <- arbitrary[(Block.Hash, Int)]
        checkpointBlockInfo <- arbitrary[(Block.Hash, Int)]
      } yield AncestryRequest(requestId, targetBlockInfo, checkpointBlockInfo)
    }

  implicit val arbAncestryResponse: Arbitrary[AncestryResponse] =
    Arbitrary {
      for {
        requestId  <- arbitrary[UUID]
        isAncestry <- arbitrary[Boolean]
      } yield AncestryResponse(requestId, isAncestry)
    }

  implicit val arbNewCheckpointCertificate
      : Arbitrary[NewCheckpointCertificate] =
    Arbitrary {
      for {
        requestId             <- arbitrary[UUID]
        checkpointCertificate <- arbitrary[CheckpointCertificate]
      } yield NewCheckpointCertificate(requestId, checkpointCertificate)
    }

  implicit val arbInterpreterMessage: Arbitrary[InterpreterMessage] =
    Arbitrary {
      Gen.oneOf(
        arbitrary[NewProposerBlockRequest],
        arbitrary[NewCheckpointCandidateRequest],
        arbitrary[CreateBlockBodyRequest],
        arbitrary[CreateBlockBodyResponse],
        arbitrary[ValidateBlockBodyRequest],
        arbitrary[ValidateBlockBodyResponse],
        arbitrary[NewCheckpointCertificateRequest],
        arbitrary[ValidExtensionRequest],
        arbitrary[ValidExtensionResponse],
        arbitrary[AncestryRequest],
        arbitrary[AncestryResponse],
        arbitrary[NewCheckpointCertificate]
      )
    }
}
