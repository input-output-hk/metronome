package io.iohk.metronome.networking

import cats.effect.Resource
import monix.eval.Task
import monix.execution.Scheduler
import org.bouncycastle.crypto.AsymmetricCipherKeyPair
import org.scalatest.Assertion
import scodec.Codec
import scodec.bits.BitVector

import java.net.{InetSocketAddress, ServerSocket}
import java.security.SecureRandom
import scala.concurrent.Future
import scala.util.Random

object RemoteConnectionManagerTestUtils {
  def customTestCaseResourceT[T](
      fixture: Resource[Task, T]
  )(theTest: T => Task[Assertion])(implicit s: Scheduler): Future[Assertion] = {
    fixture.use(fix => theTest(fix)).runToFuture
  }

  def customTestCaseT[T](
      test: => Task[Assertion]
  )(implicit s: Scheduler): Future[Assertion] = {
    test.runToFuture
  }

  def randomAddress(): InetSocketAddress = {
    val s = new ServerSocket(0)
    try {
      new InetSocketAddress("localhost", s.getLocalPort)
    } finally {
      s.close()
    }
  }

  import scodec.codecs._

  sealed abstract class TestMessage
  case class MessageA(i: Int)    extends TestMessage
  case class MessageB(s: String) extends TestMessage

  object TestMessage {
    implicit val messageCodec: Codec[TestMessage] = discriminated[TestMessage]
      .by(uint8)
      .typecase(1, int32.as[MessageA])
      .typecase(2, utf8.as[MessageB])
  }

  case class Secp256k1Key(key: BitVector)

  object Secp256k1Key {
    implicit val codec: Codec[Secp256k1Key] = bits.as[Secp256k1Key]

    def getFakeRandomKey: Secp256k1Key = {
      val array = new Array[Byte](64)
      Random.nextBytes(array)
      Secp256k1Key(BitVector(array))
    }

  }

  case class NodeInfo(keyPair: AsymmetricCipherKeyPair, publicKey: Secp256k1Key)

  object NodeInfo {
    def generateRandom(secureRandom: SecureRandom): NodeInfo = {
      val keyPair = CryptoUtils.generateSecp256k1KeyPair(secureRandom)
      NodeInfo(
        keyPair,
        Secp256k1Key(CryptoUtils.secp256k1KeyPairToNodeId(keyPair))
      )
    }
  }
}
