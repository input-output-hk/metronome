package io.iohk.metronome.checkpointing.service.messages

import io.iohk.metronome.hotstuff
import io.iohk.metronome.checkpointing
import io.iohk.metronome.checkpointing.CheckpointingAgreement

/** Capture the messages that go between checkpointing nodes.
  *
  * This is the type we can use with the `RemoteConnectionManager`.
  */
sealed trait RemoteMessage

object RemoteMessage {

  /** Messages which are part of the basic HotStuff BFA algorithm.
    *
    * These messages will be handled by the HotStuff Service.
    */
  case class HotStuffMessage(
      message: hotstuff.service.messages.ProtocolMessage[CheckpointingAgreement]
  ) extends RemoteMessage

  /** Messages which are not part of the HotStuff BFT domain,
    * but specific to what the concrete use case the aggrement
    * is about, which is checkpointing.
    *
    * For example not every application of HotStuff has to have
    * a ledger, with a state that can be synchronised directly.
    * Maybe some use cases need to synchronise and run all blocks
    * from genesis.
    *
    * These messages will be handled by the Checkpointing Service.
    */
  case class CheckpointingMessage(
      message: checkpointing.service.messages.CheckpointingMessage
  ) extends RemoteMessage
}
