package io.iohk.metronome.checkpointing.models

import io.iohk.metronome.crypto.hash.Hash

/** ETCBlock is a simpler version of original ETC block in the chain that
  * supports the running of the checkpointing interpreter.
  */
case class ETCBlock(
    header: ETCBlock.Header,
    body: ETCBlock.Body
) {
  def hash: Hash = header.hash
}

object ETCBlock {

  def makeUnsafe(header: Header, body: Body): ETCBlock =
    ETCBlock(header, body)

  case class Header(
      parentHash: Hash,
      hash: Hash,
      blockNumber: BigInt
  )

  object Header

  case class Body(
      transactions: Seq[Hash]
  )

  object Body {
    val empty: Body = Body(Seq.empty[Hash])
  }

}
