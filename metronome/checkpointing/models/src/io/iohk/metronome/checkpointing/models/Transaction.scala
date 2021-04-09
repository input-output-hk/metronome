package io.iohk.metronome.checkpointing.models

import scodec.bits.BitVector

/** Transactions are what comprise the block body used by the Checkpointing Service.
  *
  * The HotStuff BFT Agreement doesn't need to know about them, their execution and
  * validation is delegated to the Checkpointing Service, which, in turn, delegates
  * to the interpreter. The only component that truly has to understand the contents
  * is the PoW specific interpreter.
  *
  * What the Checkpointing Service has to know is the different kinds of transactions
  * we support, which is to register proposer blocks in the ledger, required by Advocate,
  * and to register checkpoint candidates.
  */
sealed trait Transaction extends RLPHash[Transaction, Transaction.Hash]

object Transaction
    extends RLPHashCompanion[Transaction]()(RLPCodecs.rlpTransaction) {

  /** In PoW chains that support Advocate checkpointing, the Checkpoint Certificate
    * can enforce the inclusion of proposed blocks on the chain via references; think
    * uncle blocks that also get executed.
    *
    * In order to know which proposed blocks can be enforced, i.e. ones that are valid
    * and have saturated the network, first the federation members need to reach BFT
    * agreement over the list of existing proposer blocks.
    *
    * The `ProposerBlock` transaction adds one of these blocks that exist on the PoW
    * chain to the Checkpointing Ledger, iff it can be validated by the members.
    *
    * The contents of the transaction are opaque, they only need to be understood
    * by the PoW side interpreter.
    *
    * Using Advocate is optional; if the PoW chain doesn't support references,
    * it will just use `CheckpointCandidate` transactions.
    */
  case class ProposerBlock(value: BitVector) extends Transaction

  /** When a federation member is leading a round, it will ask the PoW side interpreter
    * if it wants to propose a checkpoint candidate. The interpreter decides if the
    * circumstances are right, e.g. enough new blocks have been build on the previous
    * checkpoint that a new one has to be issued. If so, a `CheckpointCandidate`
    * transaction is added to the next block, which is sent to the HotStuff replicas
    * in a `Prepare` message, to be validated and committed.
    *
    * If the BFT agreement is successful, a Checkpoint Certificate will be formed
    * during block execution which will include the `CheckpointCandidate`.
    *
    * The contents of the transaction are opaque, they only need to be understood
    * by teh PoW side interpreter, either for validation, or for following the
    * fork indicated by the checkpoint.
    */
  case class CheckpointCandidate(value: BitVector) extends Transaction
}
