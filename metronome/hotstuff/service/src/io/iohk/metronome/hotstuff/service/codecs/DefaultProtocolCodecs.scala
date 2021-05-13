package io.iohk.metronome.hotstuff.service.codecs

import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.hotstuff.consensus.basic.{Message, QuorumCertificate}
import io.iohk.metronome.hotstuff.consensus.basic.VotingPhase
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import scodec.Codec
import scodec.codecs._

trait DefaultProtocolCodecs[A <: Agreement] { self: DefaultConsensusCodecs[A] =>

  implicit def groupSignatureCodec: Codec[Agreement.GroupSignature[A]]

  implicit def partialSignatureCodec: Codec[Agreement.PartialSignature[A]]

  implicit def blockCodec: Codec[A#Block]

  // Derivation doesn't work for Quorum and Vote.
  implicit val quorumCertificateCodec: Codec[QuorumCertificate[A]] =
    (("phase" | phaseCodec) ::
      ("viewNumber" | viewNumberCodec) ::
      ("blockHash" | hashCodec) ::
      ("signature" | groupSignatureCodec))
      .as[
        (
            VotingPhase,
            ViewNumber,
            A#Hash,
            Agreement.GroupSignature[A]
        )
      ]
      .xmap(
        (QuorumCertificate.apply[A] _).tupled,
        QuorumCertificate.unapply[A](_).get
      )

  implicit val prepareCodec: Codec[Message.Prepare[A]] =
    Codec.deriveLabelledGeneric

  implicit val voteCodec: Codec[Message.Vote[A]] =
    (("viewNumber" | viewNumberCodec) ::
      ("phase" | phaseCodec) ::
      ("blockHash" | hashCodec) ::
      ("signature" | partialSignatureCodec))
      .as[
        (
            ViewNumber,
            VotingPhase,
            A#Hash,
            Agreement.PartialSignature[A]
        )
      ]
      .xmap(
        (Message.Vote.apply[A] _).tupled,
        Message.Vote.unapply[A](_).get
      )

  implicit val quorumCodec: Codec[Message.Quorum[A]] =
    Codec.deriveLabelledGeneric

  implicit val newViewCodec: Codec[Message.NewView[A]] =
    Codec.deriveLabelledGeneric

  implicit val protocolMessageCodec: Codec[Message[A]] =
    discriminated[Message[A]]
      .by(uint8)
      .typecase(0, prepareCodec)
      .typecase(1, voteCodec)
      .typecase(2, quorumCodec)
      .typecase(3, newViewCodec)

}
