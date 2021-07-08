package io.iohk.metronome.hotstuff.service.codecs

import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.{
  Message,
  QuorumCertificate,
  Agreement,
  VotingPhase,
  Phase
}
import scodec.{Attempt, Codec, Err}
import scodec.codecs._
import scala.reflect.ClassTag

trait DefaultProtocolCodecs[A <: Agreement] { self: DefaultConsensusCodecs[A] =>

  implicit def groupSignatureCodec: Codec[Agreement.GroupSignature[A]]

  implicit def partialSignatureCodec: Codec[Agreement.PartialSignature[A]]

  implicit def blockCodec: Codec[A#Block]

  // Derivation doesn't work for Quorum and Vote.
  implicit val quorumCertificateCodec
      : Codec[QuorumCertificate[A, VotingPhase]] =
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
        (QuorumCertificate.apply[A, VotingPhase] _).tupled,
        QuorumCertificate.unapply[A, VotingPhase](_).get
      )

  def quorumCertificateCodecPhase[P <: VotingPhase: ClassTag] = {
    val ct = implicitly[ClassTag[P]]
    quorumCertificateCodec.exmap[QuorumCertificate[A, P]](
      qc =>
        ct.unapply(qc.phase) match {
          case Some(_) => Attempt.successful(qc.coerce[P])
          case None =>
            Attempt.failure(
              Err(
                s"Invalid phase ${qc.phase}, expected ${ct.runtimeClass.getSimpleName}"
              )
            )
        },
      qc => Attempt.successful(qc)
    )
  }

  implicit val quorumCertificateCodecPrepare
      : Codec[QuorumCertificate[A, Phase.Prepare]] =
    quorumCertificateCodecPhase[Phase.Prepare]

  implicit val quorumCertificateCodecCommit
      : Codec[QuorumCertificate[A, Phase.Commit]] =
    quorumCertificateCodecPhase[Phase.Commit]

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
      .by(uint2)
      .typecase(0, prepareCodec)
      .typecase(1, voteCodec)
      .typecase(2, quorumCodec)
      .typecase(3, newViewCodec)

}
