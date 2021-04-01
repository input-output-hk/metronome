package io.iohk.metronome.checkpointing.models

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
  ) = makeUnsafe(
    header = header.copy(
      bodyHash = body.hash,
      // TODO (PM-3102): Compute Root Hash over the transactions.
      contentMerkleRoot = MerkleTree.Hash.empty
    ),
    body = body
  )

  case class Header(
      parentHash: Header.Hash,
      // Hash of the Ledger after executing the block.
      postStateHash: Ledger.Hash,
      // Hash of the transactions in the body.
      bodyHash: Body.Hash,
      // Merkle root of the transactions in the body.
      // TODO (PM-3102): Should this just replace the `bodyHash`?
      contentMerkleRoot: MerkleTree.Hash
  ) extends RLPHash[Header, Header.Hash]

  object Header extends RLPHashCompanion[Header]()(RLPCodecs.rlpBlockHeader)

  case class Body(
      transactions: IndexedSeq[Transaction]
  ) extends RLPHash[Body, Body.Hash]

  object Body extends RLPHashCompanion[Body]()(RLPCodecs.rlpBlockBody)
}
