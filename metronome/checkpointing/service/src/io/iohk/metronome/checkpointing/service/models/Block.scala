package io.iohk.metronome.checkpointing.service.models

import io.iohk.metronome.checkpointing.interpreter.models.Transaction
import io.iohk.metronome.crypto.hash.Hash

/** Represents what the HotStuff paper called "nodes" as the "tree",
  * with the transactions in the body being the "commands".
  *
  * The block contents are specific to the checkpointing application.
  */
sealed abstract case class Block(
    header: Block.Header,
    body: Block.Body
) {
  def hash: Hash = header.hash
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
      parentHash: Hash,
      // Hash of the Ledger before executing the block.
      preStateHash: Hash,
      // Hash of the Ledger after executing the block.
      postStateHash: Hash,
      // Hash of the transactions in the body.
      bodyHash: Hash
  ) extends RLPHash[Header] {
    protected override val encoder = RLPCodecs.rlpBlockHeader
  }

  case class Body(
      transactions: Vector[Transaction]
  ) extends RLPHash[Body] {
    protected override val encoder = RLPCodecs.rlpBlockBody
  }
}
