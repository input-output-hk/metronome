package io.iohk.metronome.examples.robot.codecs

import io.iohk.ethereum.crypto.ECDSASignature
import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.examples.robot.RobotAgreement
import io.iohk.metronome.hotstuff.consensus.basic.{Message, QuorumCertificate}
import io.iohk.metronome.hotstuff.consensus.basic.VotingPhase
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import scodec.{Codec, Attempt, Err}
import scodec.bits.BitVector
import scodec.codecs._

trait RobotProtocolCodecs { self: RobotConsensusCodecs with RobotModelCodecs =>
  import scodec.codecs.implicits._

  implicit val ecdsaSignatureCodec: Codec[ECDSASignature] = {
    import akka.util.ByteString
    Codec[BitVector].exmap(
      bits =>
        Attempt.fromOption(
          ECDSASignature.fromBytes(ByteString.fromArray(bits.toByteArray)),
          Err("Not a valid signature.")
        ),
      sig => Attempt.successful(BitVector(sig.toBytes.toArray[Byte]))
    )
  }

  implicit val groupSignatureCodec: Codec[RobotAgreement.GroupSignature] =
    Codec.deriveLabelledGeneric

  implicit val partialSignatureCodec: Codec[RobotAgreement.PartialSignature] =
    Codec.deriveLabelledGeneric

  // Derivation doesn't work for Quorum and Vote.
  implicit val quorumCertificateCodec
      : Codec[QuorumCertificate[RobotAgreement]] =
    (("phase" | Codec[VotingPhase]) ::
      ("viewNumber" | Codec[ViewNumber]) ::
      ("blockHash" | Codec[Hash]) ::
      ("signature" | Codec[RobotAgreement.GroupSignature]))
      .as[
        (
            VotingPhase,
            ViewNumber,
            Hash,
            RobotAgreement.GroupSignature
        )
      ]
      .xmap(
        (QuorumCertificate.apply[RobotAgreement] _).tupled,
        QuorumCertificate.unapply[RobotAgreement](_).get
      )

  implicit val prepareCodec: Codec[Message.Prepare[RobotAgreement]] =
    Codec.deriveLabelledGeneric

  implicit val voteCodec: Codec[Message.Vote[RobotAgreement]] =
    (("viewNumber" | Codec[ViewNumber]) ::
      ("phase" | Codec[VotingPhase]) ::
      ("blockHash" | Codec[Hash]) ::
      ("signature" | Codec[RobotAgreement.PartialSignature]))
      .as[
        (
            ViewNumber,
            VotingPhase,
            Hash,
            RobotAgreement.PartialSignature
        )
      ]
      .xmap(
        (Message.Vote.apply[RobotAgreement] _).tupled,
        Message.Vote.unapply[RobotAgreement](_).get
      )

  implicit val quorumCodec: Codec[Message.Quorum[RobotAgreement]] =
    Codec.deriveLabelledGeneric

  implicit val newViewCodec: Codec[Message.NewView[RobotAgreement]] =
    Codec.deriveLabelledGeneric

  implicit val protocolMessageCodec: Codec[Message[RobotAgreement]] =
    discriminated[Message[RobotAgreement]]
      .by(uint8)
      .typecase(0, prepareCodec)
      .typecase(1, voteCodec)
      .typecase(2, quorumCodec)
      .typecase(3, newViewCodec)

}
