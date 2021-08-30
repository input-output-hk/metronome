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

  /** Messages from the Chain to the Interpreter */
  sealed trait FromChain

  /** Mark requests that require no response. */
  sealed trait NoResponse { self: Request => }

  /** Service <=> Interpreter */

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
    * In that round, the Service should send a `CreateBlockBodyRequest`.
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
    * The reason the mempool has to be sent to the Interpreter
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
    *
    * Another reason the ledger and mempool are sent and not
    * handled inside the Interpreter alone is because the Service
    * can project the correct values based on what (potentially
    * uncommitted) parent block it's currently trying to extends,
    * by updating the last stable ledger and filtering the mempool
    * based on the blocks in the tentative branch. The Interpreter
    * doesn't have access to the block history, so it couldn't do
    * the same on its own.
    */
  case class CreateBlockBodyRequest(
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
    *
    * The response can also contain a set of mempool items that
    * should be permanently removed, because they will never be
    * included in a block body. This should prevent pending
    * transactions lingering forever in memory.
    */
  case class CreateBlockBodyResponse(
      requestId: RequestId,
      blockBody: Block.Body,
      purgeFromMempool: Set[Transaction.ProposerBlock]
  ) extends InterpreterMessage
      with Response
      with FromInterpreter

  /** The Service asks the Interpreter to validate all transactions
    * in a block, given the current ledger state.
    *
    * This could be done transaction by transaction, but that would
    * require sending the ledger every step along the way, which
    * would be less efficient.
    *
    * If the Interpreter doesn't have enough data to validate the
    * block, it should hold on to it until it does, only responding
    * when it has the final conclusion.
    *
    * If the transactions are valid, the Service will apply them
    * on the ledger on its own; the update rules are transparent.
    */
  case class ValidateBlockBodyRequest(
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
  case class ValidateBlockBodyResponse(
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

  /** Interpreter <=> Chain */

  /** The interpreter requests the chain for block at target height in order to construct
    * the service block
    */
  case class CheckpointBlockRequest(
      requestId: RequestId,
      blockNumber: Long
  ) extends InterpreterMessage
      with Request
      with FromInterpreter

  /** The chain replies to @CheckpointBlockRequest with block metadata */
  case class CheckpointBlockResponse(
      requestId: RequestId,
      blockMetadata: Block.Header
  ) extends InterpreterMessage
      with Response
      with FromChain

  /** The chain requests the interpreter to perform a check about whether its latest
    * received block extends from the latest checkpointed block.
    *
    * This extension validation should be called after the block parent is verified to
    * exist the chain.
    */
  case class ValidExtensionRequest(
      requestId: RequestId,
      blockHeader: Block.Header
  ) extends InterpreterMessage
      with Request
      with FromChain

  /** The interpreter replies to the chain with a boolean value indicating the relationship */

  case class ValidExtensionResponse(
      requestId: RequestId,
      isExtended: Boolean
  ) extends InterpreterMessage
      with Response
      with FromInterpreter

  /** The interpreter asks the chain whether the target block is an immediate family of
    * the checkpointed block. This message communication takes place after the interpreter gets
    * @ValidExtensionRequest and it is needed because the interpreter stores all checkpointing records
    * but do have information about the chain. Furthermore, duplicate the chain's metadata is not necessary
    * because the interpreter serves as a plugin and it is assumed that the local message passing between
    * the interpreter and the chain is quick and safe.
    */

  case class AncestryRequest(
      requestId: RequestId,
      targetBlockInfo: Block.Hash,
      checkpointBlockInfo: Block.Hash
  ) extends InterpreterMessage
      with Request
      with FromInterpreter

  /** The chain replies to the interpreter with a boolean value indicating the relationship */
  case class AncestryResponse(
      requestId: RequestId,
      isAncestry: Boolean
  ) extends InterpreterMessage
      with Response
      with FromChain

  /** After extension check, the chain sends block metadata to the interpreter for it to detect any checkpoint
    * candidate. This message is required because previously the interpreter only knows about the block hash as well
    * as the chain number.
    */

  case class NewBlockMetadata(
      requestId: RequestId,
      blockMetadata: Block.Header
  ) extends InterpreterMessage
      with Request
      with FromChain
      with NoResponse

  /** Once the interpreter have received a new checkpoint certificate from the service,
    * it transmits the certificate to the chain in order to reorg the chain's structure if
    * necessary.
    *
    * In the document, the interpreter forwards the entire certificate to the chain which I
    * think is not necessary. What's key about it is the list of checkpointed block hashes.
    * Hence, I would suggest the content of this message be a list of hashes instead and there is
    * no need to worry about the verification because the interpreter checks that ahead.
    */
  case class NewCheckpointCertificate(
      requestId: RequestId,
      certificate: CheckpointCertificate
  ) extends InterpreterMessage
      with Request
      with FromInterpreter
      with NoResponse

  /** During the chain reorganizing its structure under a new checkpoint certificate, it needs
    * to acquire the second last checkpointed block from the interpreter as the starting point.
    */
  case class PenUltimateCheckpointRequest(
      requestId: RequestId
  ) extends InterpreterMessage
      with Request
      with FromChain

  /** The message serves as the reply to @PenUltimateCheckpointRequest sent by the chain */
  case class PenUltimateCheckpointResponse(
      requestId: RequestId,
      prevCheckpointBlock: Block.Hash
  ) extends InterpreterMessage
      with Response
      with FromInterpreter

  implicit val createBlockBodyPair =
    pair[CreateBlockBodyRequest, CreateBlockBodyResponse]

  implicit val validateBlockBodyPair =
    pair[ValidateBlockBodyRequest, ValidateBlockBodyResponse]

  implicit val checkpointBlockPair =
    pair[CheckpointBlockRequest, CheckpointBlockResponse]

  implicit val validateExtensionPair =
    pair[ValidExtensionRequest, ValidExtensionResponse]

  implicit val ancestryPair =
    pair[AncestryRequest, AncestryResponse]

  implicit val PenUltimateCheckpointPair =
    pair[PenUltimateCheckpointRequest, PenUltimateCheckpointResponse]

}
