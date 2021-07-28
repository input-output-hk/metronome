package io.iohk.metronome.checkpointing.models

import org.scalacheck._
import org.scalacheck.Prop.forAll
import io.iohk.metronome.hotstuff.consensus.Federation
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.crypto.ECKeyPair
import java.security.SecureRandom
import io.iohk.metronome.hotstuff.consensus.LeaderSelection
import io.iohk.metronome.hotstuff.consensus.basic.Secp256k1Signing
import scodec.bits.ByteVector
import io.iohk.ethereum.rlp

object CheckpointCertificateProps extends Properties("CheckpointCertificate") {
  import ArbitraryInstances._
  import Arbitrary.arbitrary
  import RLPCodecs._

  implicit val arbFederation
      : Arbitrary[Federation[CheckpointingAgreement.PKey]] = Arbitrary {
    for {
      seed <- arbitrary[Array[Byte]]
      rnd = new SecureRandom(seed)
      fedSize <- Gen.choose(3, 10)
      keys = Vector.fill(fedSize)(ECKeyPair.generate(rnd))
      fed  = Federation(keys.map(_.pub))(LeaderSelection.RoundRobin)
    } yield fed.fold(sys.error, identity)
  }

  implicit val signing =
    new Secp256k1Signing[CheckpointingAgreement](
      (phase, viewNumber, blockHash) =>
        List(
          rlp.encode(phase),
          rlp.encode(viewNumber),
          rlp.encode(blockHash)
        ).map(ByteVector(_)).reduce(_ ++ _)
    )

  property("validate - reject random") = forAll {
    (
        federation: Federation[CheckpointingAgreement.PKey],
        certificate: CheckpointCertificate
    ) =>
      CheckpointCertificate.validate(certificate, federation).isLeft
  }
}
