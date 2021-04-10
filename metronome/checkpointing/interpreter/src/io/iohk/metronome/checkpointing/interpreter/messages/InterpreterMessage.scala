package io.iohk.metronome.checkpointing.interpreter.messages

import io.iohk.metronome.core.messages.{RPCMessage, RPCMessageCompanion}
import io.iohk.metronome.checkpointing.models.{
  Transaction,
  Ledger,
  Block,
  CheckpointCertificate
}

/** Messages exchanged between the Checkpointing Service
  * and the Checkpointing Interpreter.
  */
sealed trait InterpreterMessage { self: RPCMessage => }

object InterpreterMessage extends RPCMessageCompanion {

  /** Messages from the Service to the Interpreter. */
  sealed trait FromService

  /** Messages from the Interpreter to the Service. */
  sealed trait FromInterpreter

  /** Mark requests that require no response. */
  sealed trait NoResponse { self: Request => }

  /** The Interpreter notifies the Service about a new
    * proposer block that should be added to the mempool.
    *
    * Only used in Advocate.
    */
  case class NewProposerBlockRequest(
      requestId: RequestId,
      block: Transaction.ProposerBlock
  ) extends InterpreterMessage
      with Request
      with FromInterpreter
      with NoResponse

  /** The Interpreter signals to the Service that it can
    * potentially produce a new checkpoint candidate in
    * the next view when the replica becomes leader. In
    * that round, the Service should send a `GetCheckpointCandidateRequest`.
    *
    * This is an optimisation, so we don't send the `Ledger` in
    * futile attempts when there's no chance for a candidate to
    * be produced.
    */
  case class NewCheckpointCandidateRequest(
      requestId: RequestId
  ) extends InterpreterMessage
      with Request
      with FromInterpreter
      with NoResponse

  /** When it becomes a leader of a view, the Service asks
    * the Interpreter to produce a new checkpoint candidate,
    * given the current state of the ledger.
    *
    * A response is expected even when there's no candidate
    * to put in a block, so that we can move on to the next
    * leader after an idle round, but without a timeout.
    */
  case class GetCheckpointCandidateRequest(
      requestId: RequestId,
      ledger: Ledger
  ) extends InterpreterMessage
      with Request
      with FromService

  /** The Interpreter may or may not be able to produce a new
    * checkpoint candidate, depending on whether the conditions
    * are right (e.g. the next checkpointing height has been reached).
    *
    * The response should contain `None` if we should just move on
    * to the next leader, or `Some` if a block is to be prepared.
    */
  case class GetCheckpointCandidateResponse(
      requestId: RequestId,
      maybeCheckpointCandidate: Option[Transaction.CheckpointCandidate]
  ) extends InterpreterMessage
      with Response
      with FromInterpreter

  /** The Service asks the Interpreter to validate all transactions
    * in a block, given the current ledger state, and return the
    * updated ledger.
    *
    * This could be done transaction by transaction, but that would
    * require sending the ledger every step along the way, which
    * would be less efficient.
    *
    * The updated ledger is returned so the interpeter can decide
    * which proposer blocks to keep after applying any checkpoints.
    *
    * If the Interpreter doesn't have enough data to validate the
    * block, it should hold on to it until it does, only responding
    * when it has the final conclusion.
    */
  case class ValidateBlockRequest(
      requestId: RequestId,
      blockBody: Block.Body,
      ledger: Ledger
  ) extends InterpreterMessage
      with Request
      with FromService

  /** The Interpreter responds to the block validation request when
    * it has all the data available to perform the validation.
    *
    * If the block was valid, `Some` updated ledger is returned,
    * otherwise the response contains `None`.
    *
    * The Service will check whether the hash of the updated ledger
    * matches the `postStateHash` in the block header.
    */
  case class ValidateBlockResponse(
      requestId: RequestId,
      maybeUpdatedLedger: Option[Ledger]
  ) extends InterpreterMessage
      with Response
      with FromInterpreter {

    /** Indicate whether the block contents were valid.
      *
      * Reasons for being invalid could be that a checkpoint
      * was proposed which is inconsistent with the current ledger,
      * or that a proposer block was pointing at an invalid block.
      */
    def isValid = maybeUpdatedLedger.isDefined
  }

  /** The Service notifies the Interpreter about a new Checkpoint Certificate
    * having been constructed, after a block had been committed that resulted
    * in the commit of a checkpoint candidate.
    *
    * The certificate is created by the Service because it has access to all the
    * block headers an quorum certificates, and thus can construct the Merkle proof.
    */
  case class NewCheckpointCertificateRequest(
      requestId: RequestId,
      checkpointCertificate: CheckpointCertificate
  ) extends InterpreterMessage
      with Request
      with FromService
      with NoResponse
}
