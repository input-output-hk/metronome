package io.iohk.metronome.hotstuff.consensus.basic

/** Type class to project the properties we need a HotStuff block to have
  * from the generic `Block` type in the `Agreement`.
  *
  * This allows the block to include use-case specific details HotStuff doesn't
  * care about, for example to build up a ledger state that can be synchronised
  * directly, rather than just carry out a sequence of commands on all replicas.
  * This would require the blocks to contain ledger state hashes, which other
  * use cases may have no use for.
  */
trait Block[A <: Agreement] {
  def blockHash(b: A#Block): A#Hash
  def parentBlockHash(b: A#Block): A#Hash

  /** Perform simple content validation. */
  def isValid(b: A#Block): Boolean
}

object Block {
  def apply[A <: Agreement: Block]: Block[A] = implicitly[Block[A]]
}
