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
import org.scalacheck.Prop.{forAll, forAllNoShrink, propBoolean}
import scodec.bits.ByteVector
import scala.util.Random

object CheckpointCertificateProps extends Properties("CheckpointCertificate") {
  import ArbitraryInstances._
  import Arbitrary.arbitrary
  import RLPCodecs._

  // Testing with real signatures is rather slow.
  override def overrideParameters(p: Test.Parameters): Test.Parameters =
    p.withMinSuccessfulTests(25)

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

  def invalidateCertificate(
      c: CheckpointCertificate
  ): Gen[(String, CheckpointCertificate)] =
    Gen.oneOf(
      arbitrary[Block.Hash].map { h =>
        "Invalid block hash" -> c.copy(commitQC =
          c.commitQC.copy[CheckpointingAgreement, Phase.Commit](blockHash = h)
        )
      },
      arbitrary[ViewNumber].map { vn =>
        "Invalid view number" -> c.copy(commitQC =
          c.commitQC.copy[CheckpointingAgreement, Phase.Commit](viewNumber =
            ViewNumber(c.commitQC.viewNumber + vn)
          )
        )
      },
      Gen.oneOf(c.commitQC.signature.sig).map { s =>
        "Removed signature" -> c.copy(commitQC =
          c.commitQC.copy[CheckpointingAgreement, Phase.Commit](signature =
            c.commitQC.signature.copy(
              sig = c.commitQC.signature.sig.filterNot(_ == s)
            )
          )
        )
      },
      arbitrary[CheckpointingAgreement.PSig].map { s =>
        "Replaced signature" -> c.copy(commitQC =
          c.commitQC.copy[CheckpointingAgreement, Phase.Commit](signature =
            c.commitQC.signature.copy(
              sig = s +: c.commitQC.signature.sig.tail
            )
          )
        )
      },
      arbitrary[Transaction.CheckpointCandidate].map { cc =>
        "Invalid candidate" -> c.copy(checkpoint = cc)
      },
      arbitrary[CheckpointCertificate].map { cc =>
        "Invalid proof" -> c.copy(proof = cc.proof)
      },
      for {
        hs0 <- Gen.const(c.headers.toList)
        if hs0.size > 1
        seed <- arbitrary[Int]
        hs1 = new Random(seed).shuffle(hs0)
        if hs1 != hs0
      } yield "Shuffled headers" -> c.copy(headers =
        NonEmptyList.fromListUnsafe(hs1)
      ),
      Gen.nonEmptyListOf(arbitrary[Block.Header]).map { bs =>
        "Prefixed headers" ->
          c.copy(headers = NonEmptyList.fromListUnsafe(bs ++ c.headers.toList))
      },
      Gen.nonEmptyListOf(arbitrary[Block.Header]).map { bs =>
        "Postfixed headers" ->
          c.copy(headers = NonEmptyList.fromListUnsafe(c.headers.toList ++ bs))
      }
    )

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

  property("validate - reject invalid") = forAllNoShrink(
    for {
      (federation, valid) <- validCertificate
      (hint, invalid)     <- invalidateCertificate(valid)
    } yield (federation, hint, invalid)
  ) { case (federation, hint, certificate) =>
    hint |: CheckpointCertificate.validate(certificate, federation).isLeft
  }

  property("validate - reject foreign") = forAll(
    for {
      (_, valid) <- validCertificate
      federation <- arbitrary[Federation[CheckpointingAgreement.PKey]]
    } yield (federation, valid)
  ) { case (federation, certificate) =>
    CheckpointCertificate.validate(certificate, federation).isLeft
  }
}
