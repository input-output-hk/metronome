package io.iohk.metronome.checkpointing.interpreter.messages

import io.iohk.metronome.core.messages.{RPCMessage, RPCMessageCompanion}
import io.iohk.metronome.checkpointing.models.{
  Transaction,
  Ledger,
  Block,
  CheckpointCertificate
}

/** Messages exchanged between the Checkpointing Service
  * and the local Checkpointing Interpreter.
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
      proposerBlock: Transaction.ProposerBlock
  ) extends InterpreterMessage
      with Request
      with FromInterpreter
      with NoResponse

  /** The Interpreter signals to the Service that it can
    * potentially produce a new checkpoint candidate in
    * the next view when the replica becomes leader.
    *
    * In that round, the Service should send a `CreateBlockRequest`.
    *
    * This is a potential optimization, so we don't send the `Ledger`
    * in futile attempts when there's no chance for a block to
    * be produced when there have been no events.
    */
  case class NewCheckpointCandidateRequest(
      requestId: RequestId
  ) extends InterpreterMessage
      with Request
      with FromInterpreter
      with NoResponse

  /** When it becomes a leader of a view, the Service asks
    * the Interpreter to produce a new block body, populating
    * it with transactions in the correct order, based on
    * the current ledger and the mempool.
    *
    * A response is expected even when there are no transactions
    * to be put in a block, so that we can move on to the next
    * leader after an idle round (agreeing on an empty block),
    * without incurring a full timeout.
    *
    * The reason the mempool has to be sent to the interpreter
    * and not just appended to the block, with a potential
    * checkpoint at the end, is because the checkpoint empties
    * the Ledger, and the Service has no way of knowing whether
    * all proposer blocks have been rightly checkpointed. The
    * Interpreter, on the other hand, can put the checkpoint
    * in the correct position in the block body, and make sure
    * that proposer blocks which cannot be checkpointed yet are
    * added in a trailing position.
    *
    * The mempool will be eventually cleared by the Service as
    * blocks are executed, based on what transactions they have.
    */
  case class CreateBlockRequest(
      requestId: RequestId,
      ledger: Ledger,
      mempool: Seq[Transaction.ProposerBlock]
  ) extends InterpreterMessage
      with Request
      with FromService

  /** The Interpreter may or may not be able to produce a new
    * checkpoint candidate, depending on whether the conditions
    * are right (e.g. the next checkpointing height has been reached).
    *
    * The response should contain an empty block body if there is
    * nothing to do, so the Service can either propose an empty block
    * to keep everyone in sync, or just move to the next leader by
    * other means.
    */
  case class CreateBlockResponse(
      requestId: RequestId,
      blockBody: Block.Body
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
    * The result indicates whether the block contents were valid.
    *
    * Reasons for being invalid could be that a checkpoint
    * was proposed which is inconsistent with the current ledger,
    * or that a proposer block was pointing at an invalid block.
    *
    * If valid, the Service updates its copy of the ledger
    * and checks that the `postStateHash` in the block also
    * corresponds to its state.
    */
  case class ValidateBlockResponse(
      requestId: RequestId,
      isValid: Boolean
  ) extends InterpreterMessage
      with Response
      with FromInterpreter

  /** The Service notifies the Interpreter about a new Checkpoint Certificate
    * having been constructed, after a block had been committed that resulted
    * in the commit of a checkpoint candidate.
    *
    * The certificate is created by the Service because it has access to all the
    * block headers and quorum certificates, and thus can construct the Merkle proof.
    */
  case class NewCheckpointCertificateRequest(
      requestId: RequestId,
      checkpointCertificate: CheckpointCertificate
  ) extends InterpreterMessage
      with Request
      with FromService
      with NoResponse

  implicit val createBlockPair =
    pair[CreateBlockRequest, CreateBlockResponse]

  implicit val validateBlockPair =
    pair[ValidateBlockRequest, ValidateBlockResponse]
}
