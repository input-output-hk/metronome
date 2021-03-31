package io.iohk.metronome.checkpointing.service.models

import io.iohk.metronome.checkpointing.interpreter.models.Transaction

/** Represents what the HotStuff paper called "nodes" as the "tree",
  * with the transactions in the body being the "commands".
  *
  * The block contents are specific to the checkpointing application.
  *
  * The header and body are separated because headers have to part
  * of the Checkpoint Certificate; there's no need to repeat all
  * the transactions there, the Merkle root will make it possible
  * to prove that a given CheckpointCandidate transaction was
  * indeed part of the block. The headers are needed for parent-child
  * validation in the certificate as well.
  */
sealed abstract case class Block(
    header: Block.Header,
    body: Block.Body
) {
  def hash: Block.Header.Hash = header.hash
}

object Block {

  /** Create a from a header and body we received from the network.
    * It will need to be validated before it can be used, to make sure
    * the header really belongs to the body.
    */
  def makeUnsafe(header: Header, body: Body): Block =
    new Block(header, body) {}

  /** Create a block from a header and a body, updating the `bodyHash` in the
    * header to make sure the final block hash is valid.
    */
  def make(
      header: Header,
      body: Body
  ) = makeUnsafe(header = header.copy(bodyHash = body.hash), body = body)

  case class Header(
      parentHash: Header.Hash,
      // Hash of the Ledger before executing the block.
      preStateHash: Ledger.Hash,
      // Hash of the Ledger after executing the block.
      postStateHash: Ledger.Hash,
      // Hash of the transactions in the body.
      bodyHash: Body.Hash
      // TODO (PM-3102): Add merkle root for contents.
      // Instead of the hash of the body, should we use the
      // the Merkle root of the transactions?
      // Or should that be an additional field?
  ) extends RLPHash[Header, Header.Hash]

  object Header extends RLPHashCompanion[Header]()(RLPCodecs.rlpBlockHeader)

  case class Body(
      transactions: Vector[Transaction]
  ) extends RLPHash[Body, Body.Hash]

  object Body extends RLPHashCompanion[Body]()(RLPCodecs.rlpBlockBody)
}
