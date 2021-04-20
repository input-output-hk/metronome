package io.iohk.metronome.networking

import cats.effect.Resource
import io.iohk.metronome.crypto.{ECKeyPair, ECPublicKey}

import java.net.{InetSocketAddress, ServerSocket}
import java.security.SecureRandom
import monix.eval.Task
import monix.execution.Scheduler
import org.scalatest.Assertion

import scala.concurrent.Future
import scala.util.Random
import scodec.bits.ByteVector
import scodec.Codec

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

  def getFakeRandomKey(): ECPublicKey = {
    val array = new Array[Byte](64)
    Random.nextBytes(array)
    ECPublicKey(ByteVector(array))
  }

  case class NodeInfo(keyPair: ECKeyPair)

  object NodeInfo {
    def generateRandom(secureRandom: SecureRandom): NodeInfo = {
      val keyPair = ECKeyPair.generate(secureRandom)
      NodeInfo(keyPair)
    }
  }
}
