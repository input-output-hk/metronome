package io.iohk.metronome.hotstuff.service

import cats.effect.{Bracket, Concurrent, ContextShift, Effect, Resource}
import io.iohk.metronome.hotstuff.service.RemoteConnectionManagerSpec.{
  TestMessage,
  buildTestConnectionManager,
  customTestCaseResourceT,
  customTestCaseT,
  randomAddress
}
import io.iohk.scalanet.peergroup.dynamictls.DynamicTLSPeerGroup.{
  FramingConfig,
  PeerInfo
}
import monix.eval.{Task, TaskLift}
import monix.execution.Scheduler
import org.bouncycastle.crypto.AsymmetricCipherKeyPair
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers
import scodec.Codec

import java.net.{InetSocketAddress, ServerSocket}
import java.security.SecureRandom
import scala.concurrent.Future

class RemoteConnectionManagerSpec extends AsyncFlatSpecLike with Matchers {
  implicit val testScheduler =
    Scheduler.fixedPool("RemoteConnectionManagerSpec", 16)

  behavior of "RemoteConnectionManager"

  it should "start connectionManager without any connections" in customTestCaseResourceT(
    buildTestConnectionManager[Task, TestMessage]()
  ) { connectionManager =>
    for {
      connections <- connectionManager.getAcquiredConnections
    } yield assert(connections.isEmpty)
  }

  it should "keep connections to other peer" in customTestCaseT {
    (for {
      rcm1 <- buildTestConnectionManager[Task, TestMessage]()
      rcm2 <- buildTestConnectionManager[Task, TestMessage](
        connectionsToAcquire = Set(rcm1.getLocalInfo)
      )
    } yield (rcm1, rcm2)).use { case (cm1, cm2) =>
      for {
        ac1 <- cm1.getAcquiredConnections
        ac2 <- cm2.getAcquiredConnections
      } yield assert(ac1.isEmpty)
    }
  }
}
object RemoteConnectionManagerSpec {
  val secureRandom = new SecureRandom()
  val standardFraming =
    FramingConfig.buildStandardFrameConfig(1000000, 4).getOrElse(null)
  val testIncomingQueueSize = 20

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

  def buildTestConnectionManager[F[_]: Concurrent: TaskLift, M: Codec](
      bindAddress: InetSocketAddress = randomAddress(),
      nodeKeyPair: AsymmetricCipherKeyPair =
        CryptoUtils.generateSecp256k1KeyPair(secureRandom),
      secureRandom: SecureRandom = secureRandom,
      useNativeTlsImplementation: Boolean = false,
      framingConfig: FramingConfig = standardFraming,
      maxIncomingQueueSizePerPeer: Int = testIncomingQueueSize,
      connectionsToAcquire: Set[PeerInfo] = Set()
  )(implicit
      s: Scheduler,
      cs: ContextShift[F]
  ): Resource[F, RemoteConnectionManager[F, M]] = {
    RemoteConnectionManager(
      bindAddress,
      nodeKeyPair,
      secureRandom,
      useNativeTlsImplementation,
      framingConfig,
      maxIncomingQueueSizePerPeer,
      connectionsToAcquire
    )
  }

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

}
