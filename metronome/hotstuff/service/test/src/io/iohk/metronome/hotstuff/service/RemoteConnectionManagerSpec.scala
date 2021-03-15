package io.iohk.metronome.hotstuff.service

import cats.effect.{Concurrent, ContextShift, Resource}
import io.iohk.metronome.hotstuff.service.RemoteConnectionManagerSpec._
import io.iohk.scalanet.peergroup.InetMultiAddress
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
import scala.concurrent.duration._

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

  it should "connect to other running peers" in customTestCaseT {
    (for {
      rcm1 <- buildTestConnectionManager[Task, TestMessage]()
      rcm2 <- buildTestConnectionManager[Task, TestMessage]()
      rcm3 <- buildTestConnectionManager[Task, TestMessage](
        connectionsToAcquire = Set(rcm1.getLocalInfo, rcm2.getLocalInfo)
      )
    } yield (rcm1, rcm2, rcm3)).use { case (cm1, cm2, cm3) =>
      for {
        connectionCountsResult <- waitFor3Managers(
          (cm1, 1),
          (cm2, 1),
          (cm3, 2)
        )(5.seconds)
        _ <- Task(assert(connectionCountsResult.isRight))
        (cm1ConnectionSize, cm2ConnectionSize, cm3ConnectionSize) =
          connectionCountsResult.getOrElse(null)
      } yield assert(cm3ConnectionSize == 2)
    }
  }

  it should "send message to other peers" in customTestCaseT {
    val testMessage1 = MessageA(1)
    val testMessage2 = MessageB("hello")

    (for {
      rcm1 <- buildTestConnectionManager[Task, TestMessage]()
      rcm2 <- buildTestConnectionManager[Task, TestMessage]()
      rcm3 <- buildTestConnectionManager[Task, TestMessage](
        connectionsToAcquire = Set(rcm1.getLocalInfo, rcm2.getLocalInfo)
      )
    } yield (rcm1, rcm2, rcm3)).use { case (cm1, cm2, cm3) =>
      for {
        connectionCountsResult <- waitFor3Managers(
          (cm1, 1),
          (cm2, 1),
          (cm3, 2)
        )(5.seconds)
        _ <- Task(assert(connectionCountsResult.isRight))
        receivedMessages <- Task.parMap3(
          cm1.sendMessage(cm3.getLocalInfo, testMessage1),
          cm2.sendMessage(cm3.getLocalInfo, testMessage2),
          cm3.incomingMessages.take(2).toListL
        )((_, _, received) => received)
      } yield {
        assert(receivedMessages.size == 2)
        assert(receivedMessages.contains((cm1.getLocalInfo, testMessage1)))
        assert(receivedMessages.contains((cm2.getLocalInfo, testMessage2)))

      }
    }
  }

}
object RemoteConnectionManagerSpec {
  def waitFor3Managers(
      cm1: (RemoteConnectionManager[Task, TestMessage], Int),
      cm2: (RemoteConnectionManager[Task, TestMessage], Int),
      cm3: (RemoteConnectionManager[Task, TestMessage], Int)
  )(timeOut: FiniteDuration): Task[Either[Unit, (Int, Int, Int)]] = {
    (Task
      .parMap3(
        cm1._1.getAcquiredConnections,
        cm2._1.getAcquiredConnections,
        cm3._1.getAcquiredConnections
      ) { case (cm1Connections, cm2Connections, cm3Connections) =>
        (cm1Connections.size, cm2Connections.size, cm3Connections.size)
      })
      .restartUntil {
        case (cm1ConnectionSize, cm2ConnectionSize, cm3ConnectionSize) =>
          cm1ConnectionSize == cm1._2 && cm2ConnectionSize == cm2._2 && cm3ConnectionSize == cm3._2
      }
      .timeout(timeOut)
      .attempt
      .map(result => result.left.map(_ => ()))
  }

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

  case class NodeInfo(
      address: InetSocketAddress,
      keyPair: AsymmetricCipherKeyPair
  ) {
    def toPeerInfo: PeerInfo = PeerInfo(
      CryptoUtils.secp256k1KeyPairToNodeId(keyPair),
      InetMultiAddress(address)
    )
  }

  object NodeInfo {
    def getRandomNodeInfo: NodeInfo = NodeInfo(
      randomAddress(),
      CryptoUtils.generateSecp256k1KeyPair(secureRandom)
    )
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
