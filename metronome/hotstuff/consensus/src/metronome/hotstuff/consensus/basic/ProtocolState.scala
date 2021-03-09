package metronome.hotstuff.consensus.basic

import metronome.hotstuff.consensus.{ViewNumber, Federation}
import scala.concurrent.duration.FiniteDuration

/** Basic HotStuff protocol state with the following generic parameters:
  *
  * See https://arxiv.org/pdf/1803.05069.pdf
  */
case class ProtocolState[A <: Agreement](
    viewNumber: ViewNumber,
    phase: Phase,
    ownPublicKey: A#PKey,
    federation: Federation[A#PKey],
    // Highest QC for which a replica voted Pre-Commit, because it received a Prepare Q.C. from the leader.
    prepareQC: QuorumCertificate[A],
    // Locked QC, for which a replica voted Commit, because it received a Pre-Commit Q.C. from leader.
    lockedQC: QuorumCertificate[A],
    lastExecutedBlockHash: A#Hash,
    timeout: FiniteDuration,
    // Votes gathered by the leader in this phase.
    votes: Set[Message.Vote[A]]
    // TODO: Gather stray messages to figure out who is leader?
) {

  /** Return an initial set of effects; at the minimum the timeout for the first round. */
  def init: List[Effect[A]] = ???

  /** Handle an input event, such as an incoming message.
    *
    * Return the updated state and any effects to be carried out in response.
    *
    * The sender is verified by the network layer and retrieved from the
    * lower level protocol message.
    */
  def handleEvent(
      event: Event[A]
  ): (ProtocolState[A], List[Effect[A]]) = ???
}
