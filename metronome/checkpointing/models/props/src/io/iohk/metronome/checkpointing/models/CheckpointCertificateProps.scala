package io.iohk.metronome.checkpointing.models

import cats.data.NonEmptyList
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.crypto.ECKeyPair
import io.iohk.ethereum.rlp
import io.iohk.metronome.hotstuff.consensus.{
  LeaderSelection,
  Federation,
  ViewNumber
}
import io.iohk.metronome.hotstuff.consensus.basic.{
  Secp256k1Signing,
  Phase,
  QuorumCertificate
}
import java.security.SecureRandom
import org.scalacheck._
import org.scalacheck.Prop.{forAll, propBoolean}
import scodec.bits.ByteVector

object CheckpointCertificateProps extends Properties("CheckpointCertificate") {
  import ArbitraryInstances._
  import Arbitrary.arbitrary
  import RLPCodecs._

  implicit val genFederation: Gen[
    (
        Federation[CheckpointingAgreement.PKey],
        Vector[CheckpointingAgreement.SKey]
    )
  ] =
    for {
      seed <- arbitrary[Array[Byte]]
      rnd = new SecureRandom(seed)
      fedSize <- Gen.choose(3, 10)
      keys = Vector.fill(fedSize)(ECKeyPair.generate(rnd))
      fed  = Federation(keys.map(_.pub))(LeaderSelection.RoundRobin)
    } yield fed.fold(sys.error, identity) -> keys.map(_.prv)

  implicit val arbFederation =
    Arbitrary(genFederation.map(_._1))

  implicit val signing =
    new Secp256k1Signing[CheckpointingAgreement](
      (phase, viewNumber, blockHash) =>
        List(
          rlp.encode(phase),
          rlp.encode(viewNumber),
          rlp.encode(blockHash)
        ).map(ByteVector(_)).reduce(_ ++ _)
    )

  def buildChain(
      n: Int,
      parentLedger: Ledger,
      ancestors: NonEmptyList[Block.Header]
  ): Gen[NonEmptyList[Block.Header]] =
    if (n == 0) {
      Gen.const(ancestors.reverse)
    } else {
      arbitrary[Vector[Transaction]].flatMap { txns =>
        val ledger = parentLedger.update(txns)
        val block  = Block.make(ancestors.head, ledger.hash, Block.Body(txns))
        buildChain(n - 1, ledger, block.header :: ancestors)
      }
    }

  val validCertificate = for {
    candidate <- arbitrary[Transaction.CheckpointCandidate]
    txnsPre   <- arbitrary[Vector[Transaction]]
    txnsPost  <- arbitrary[Vector[Transaction.ProposerBlock]]
    txns = txnsPre ++ Vector(candidate) ++ txnsPost

    parent <- arbitrary[Block.Header]
    ledger <- arbitrary[Ledger].map(_ update txns)
    block = Block.make(parent, ledger.hash, Block.Body(txns))

    chainLength <- Gen.choose(0, 5)
    chain       <- buildChain(chainLength, ledger, NonEmptyList.one(block.header))

    (federation, privateKeys) <- genFederation
    signers                   <- Gen.pick(federation.quorumSize, privateKeys)
    viewNumber                <- arbitrary[ViewNumber]
    signatures = signers.toList.map(
      signing.sign(_, Phase.Commit, viewNumber, chain.last.hash)
    )
    signature = signing.combine(signatures)

    commitQC = QuorumCertificate[CheckpointingAgreement, Phase.Commit](
      Phase.Commit,
      viewNumber,
      chain.last.hash,
      signature = signature
    )

    certificate = CheckpointCertificate.construct(block, chain, commitQC).get

  } yield (federation, certificate)

  property("validate - reject random") = forAll {
    (
        federation: Federation[CheckpointingAgreement.PKey],
        certificate: CheckpointCertificate
    ) =>
      CheckpointCertificate.validate(certificate, federation).isLeft
  }

  property("validate - accept valid") = forAll(validCertificate) {
    case (federation, certificate) =>
      CheckpointCertificate
        .validate(certificate, federation)
        .fold(
          _ |: false,
          _ => "ok" |: true
        )
  }

  property("validate - reject invalid") = true
}
