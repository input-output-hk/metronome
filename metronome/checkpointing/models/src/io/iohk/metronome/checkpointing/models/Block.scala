package io.iohk.metronome.checkpointing.models

import scodec.bits.ByteVector

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
sealed abstract case class Block private (
    header: Block.Header,
    body: Block.Body
) {
  def hash: Block.Header.Hash = header.hash
}

object Block {
  type Hash = Block.Header.Hash

  /** Create a from a header and body we received from the network.
    *
    * It will need to be validated before it can be used, to make sure
    * the header really belongs to the body.
    */
  def makeUnsafe(header: Header, body: Body): Block =
    new Block(header, body) {}

  /** Smart constructor for a block, setting the correct hashes in the header. */
  def make(
      parent: Block,
      postStateHash: Ledger.Hash,
      transactions: IndexedSeq[Transaction]
  ): Block = {
    val body = Body(transactions)
    val header = Header(
      parentHash = parent.hash,
      postStateHash = postStateHash,
      contentMerkleRoot = Body.contentMerkleRoot(body)
    )
    makeUnsafe(header, body)
  }

  /** Check that the block hashes are valid. */
  def isValid(block: Block): Boolean =
    block.header.contentMerkleRoot == Body.contentMerkleRoot(block.body)

  /** The first, empty block. */
  val genesis: Block = {
    val body = Body(Vector.empty)
    val header = Header(
      parentHash = Block.Header.Hash(ByteVector.empty),
      postStateHash = Ledger.empty.hash,
      contentMerkleRoot = MerkleTree.empty.hash
    )
    makeUnsafe(header, body)
  }

  case class Header(
      parentHash: Header.Hash,
      // Hash of the Ledger after executing the block.
      postStateHash: Ledger.Hash,
      // Merkle root of the transactions in the body.
      contentMerkleRoot: MerkleTree.Hash
  ) extends RLPHash[Header, Header.Hash]

  object Header extends RLPHashCompanion[Header]()(RLPCodecs.rlpBlockHeader)

  case class Body(
      transactions: IndexedSeq[Transaction]
  ) extends RLPHash[Body, Body.Hash]

  object Body extends RLPHashCompanion[Body]()(RLPCodecs.rlpBlockBody) {
    def contentMerkleRoot(body: Body): MerkleTree.Hash =
      MerkleTree
        .build(body.transactions.map(tx => MerkleTree.Hash(tx.hash)))
        .hash
  }
}
