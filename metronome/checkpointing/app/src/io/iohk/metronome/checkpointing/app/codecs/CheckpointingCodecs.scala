package io.iohk.metronome.checkpointing.app.codecs

import io.iohk.ethereum.rlp, rlp.RLPCodec
import io.iohk.metronome.hotstuff.service.codecs._
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.checkpointing.service.messages.CheckpointingMessage
import io.iohk.metronome.checkpointing.models.{Block, RLPCodecs, Ledger}
import scodec.{Codec, Attempt}
import scodec.codecs._
import scodec.bits.ByteVector
import scala.util.Try

object CheckpointingCodecs
    extends DefaultConsensusCodecs[CheckpointingAgreement]
    with DefaultProtocolCodecs[CheckpointingAgreement]
    with DefaultSecp256k1Codecs[CheckpointingAgreement]
    with DefaultMessageCodecs[CheckpointingAgreement]
    with DefaultDuplexMessageCodecs[
      CheckpointingAgreement,
      CheckpointingMessage
    ] {

  import scodec.codecs.implicits._
  import RLPCodecs.{rlpBlock, rlpLedger}

  private implicit def codecFromRLPCodec[T: RLPCodec]: Codec[T] =
    Codec[ByteVector].exmap(
      bytes => Attempt.fromTry(Try(rlp.decode[T](bytes.toArray))),
      value => Attempt.successful(ByteVector(rlp.encode(value)))
    )

  override implicit lazy val hashCodec: Codec[Block.Header.Hash] =
    Codec[ByteVector].xmap(Block.Header.Hash(_), identity)

  implicit lazy val ledgerHashCodec: Codec[Ledger.Hash] =
    Codec[ByteVector].xmap(Ledger.Hash(_), identity)

  override implicit lazy val blockCodec: Codec[Block] =
    codecFromRLPCodec[Block]

  implicit lazy val ledgerCodec: Codec[Ledger] =
    codecFromRLPCodec[Ledger]

  implicit lazy val getStateRequestCodec
      : Codec[CheckpointingMessage.GetStateRequest] =
    Codec.deriveLabelledGeneric

  implicit lazy val getStateResponseCodec
      : Codec[CheckpointingMessage.GetStateResponse] =
    Codec.deriveLabelledGeneric

  override implicit lazy val applicationMessageCodec
      : Codec[CheckpointingMessage] =
    discriminated[CheckpointingMessage]
      .by(uint2)
      .typecase(0, getStateRequestCodec)
      .typecase(1, getStateResponseCodec)

}
