package io.iohk.metronome.hotstuff.service.codecs

import scodec.Codec
import scodec.codecs._
import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.hotstuff.service.messages.{
  SyncMessage,
  HotStuffMessage
}

trait DefaultMessageCodecs[A <: Agreement] {
  self: DefaultConsensusCodecs[A] with DefaultProtocolCodecs[A] =>
  import scodec.codecs.implicits._

  implicit val getStatusRequestCodec: Codec[SyncMessage.GetStatusRequest[A]] =
    Codec.deriveLabelledGeneric

  implicit val getStatusResponseCodec: Codec[SyncMessage.GetStatusResponse[A]] =
    Codec.deriveLabelledGeneric

  implicit val getBlockRequestCodec: Codec[SyncMessage.GetBlockRequest[A]] =
    Codec.deriveLabelledGeneric

  implicit val getBlockResponseCodec: Codec[SyncMessage.GetBlockResponse[A]] =
    Codec.deriveLabelledGeneric

  implicit val syncMessageCodec: Codec[SyncMessage[A]] =
    discriminated[SyncMessage[A]]
      .by(uint2)
      .typecase(0, getStatusRequestCodec)
      .typecase(1, getStatusResponseCodec)
      .typecase(2, getBlockRequestCodec)
      .typecase(3, getBlockResponseCodec)

  implicit val hotstuffConsensusMessageCodec
      : Codec[HotStuffMessage.ConsensusMessage[A]] =
    Codec.deriveLabelledGeneric

  implicit val hotstuffSyncMessageCodec: Codec[HotStuffMessage.SyncMessage[A]] =
    Codec.deriveLabelledGeneric

  implicit val hotstuffMessageCodec: Codec[HotStuffMessage[A]] =
    discriminated[HotStuffMessage[A]]
      .by(uint2)
      .typecase(0, hotstuffConsensusMessageCodec)
      .typecase(1, hotstuffSyncMessageCodec)
}
