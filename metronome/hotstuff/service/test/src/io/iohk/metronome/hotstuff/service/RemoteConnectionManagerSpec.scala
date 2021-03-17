package io.iohk.metronome.hotstuff.service

import cats.effect.{Concurrent, ContextShift, Resource, Timer}
import io.iohk.metronome.hotstuff.service.RemoteConnectionManager.{
  ClusterConfig,
  MessageReceived,
  RetryConfig
}
import io.iohk.metronome.hotstuff.service.RemoteConnectionManagerSpec._
import io.iohk.scalanet.peergroup.dynamictls.DynamicTLSPeerGroup.FramingConfig
import monix.eval.{Task, TaskLift, TaskLike}
import monix.execution.Scheduler
import org.bouncycastle.crypto.AsymmetricCipherKeyPair
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers
import scodec.Codec
import scodec.bits.BitVector

import java.net.{InetSocketAddress, ServerSocket}
import java.security.SecureRandom
import scala.concurrent.Future
import scala.concurrent.duration._

class RemoteConnectionManagerSpec extends AsyncFlatSpecLike with Matchers {
  implicit val testScheduler =
    Scheduler.fixedPool("RemoteConnectionManagerSpec", 16)

  behavior of "RemoteConnectionManager"

  it should "start connectionManager without any connections" in customTestCaseResourceT(
    buildTestConnectionManager[Task, Secp256k1Key, TestMessage]()
  ) { connectionManager =>
    for {
      connections <- connectionManager.getAcquiredConnections
    } yield assert(connections.isEmpty)
  }

  it should "build fully connected cluster of 3 nodes" in customTestCaseT {
    buildFullyConnectedClusterOf3Nodes(10.seconds).use { case (cm1, cm2, cm3) =>
      for {
        cm1Peers <- cm1.getAcquiredConnections
        cm2Peers <- cm2.getAcquiredConnections
        cm3Peers <- cm3.getAcquiredConnections
      } yield assert(
        cm1Peers.size == 2 && cm2Peers.size == 2 && cm3Peers.size == 2
      )
    }
  }

  it should "send message to other peers in cluster" in customTestCaseT {
    val testMessage1 = MessageA(1)
    val testMessage2 = MessageB("hello")

    buildFullyConnectedClusterOf3Nodes(10.seconds).use { case (cm1, cm2, cm3) =>
      for {
        _ <- Task
          .parZip2(
            cm1.sendMessage(cm2.getLocalInfo._1, testMessage1),
            cm1.sendMessage(cm3.getLocalInfo._1, testMessage2)
          )
          .void
        _ <- Task
          .parZip2(
            cm2.sendMessage(cm3.getLocalInfo._1, testMessage1),
            cm2.sendMessage(cm1.getLocalInfo._1, testMessage2)
          )
          .void
        _ <- Task
          .parZip2(
            cm3.sendMessage(cm1.getLocalInfo._1, testMessage1),
            cm3.sendMessage(cm2.getLocalInfo._1, testMessage2)
          )
          .void
        cm1Received <- cm1.incomingMessages.take(2).toListL
        cm2Received <- cm2.incomingMessages.take(2).toListL
        cm3Received <- cm3.incomingMessages.take(2).toListL
      } yield {
        assert(
          cm1Received.contains(
            MessageReceived(cm2.getLocalInfo._1, testMessage2)
          ) && cm1Received.contains(
            MessageReceived(cm3.getLocalInfo._1, testMessage1)
          )
        )
        assert(
          cm2Received.contains(
            MessageReceived(cm1.getLocalInfo._1, testMessage1)
          ) && cm2Received.contains(
            MessageReceived(cm3.getLocalInfo._1, testMessage2)
          )
        )
        assert(
          cm3Received.contains(
            MessageReceived(cm1.getLocalInfo._1, testMessage2)
          ) && cm3Received.contains(
            MessageReceived(cm2.getLocalInfo._1, testMessage1)
          )
        )
      }
    }
  }

}
object RemoteConnectionManagerSpec {
  def waitFor3Managers(
      cm1: (RemoteConnectionManager[Task, Secp256k1Key, TestMessage], Int),
      cm2: (RemoteConnectionManager[Task, Secp256k1Key, TestMessage], Int),
      cm3: (RemoteConnectionManager[Task, Secp256k1Key, TestMessage], Int)
  )(timeOut: FiniteDuration): Task[Unit] = {
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
      .void
  }

  def buildFullyConnectedClusterOf3Nodes(
      timeOut: FiniteDuration
  )(implicit s: Scheduler): Resource[
    Task,
    (
        RemoteConnectionManager[Task, Secp256k1Key, TestMessage],
        RemoteConnectionManager[Task, Secp256k1Key, TestMessage],
        RemoteConnectionManager[Task, Secp256k1Key, TestMessage]
    )
  ] = {
    val kp1 = NodeInfo.generateRandom
    val kp2 = NodeInfo.generateRandom
    val kp3 = NodeInfo.generateRandom
    (for {
      rcm1 <- buildTestConnectionManager[Task, Secp256k1Key, TestMessage](
        nodeKeyPair = kp1.keyPair,
        clusterConfig = ClusterConfig
          .buildConfig(
            Set.empty[(Secp256k1Key, InetSocketAddress)],
            Set(kp2.publicKey, kp3.publicKey)
          )
          .get
      )
      rcm2 <- buildTestConnectionManager[Task, Secp256k1Key, TestMessage](
        nodeKeyPair = kp2.keyPair,
        clusterConfig = ClusterConfig
          .buildConfig(
            Set(rcm1.getLocalInfo),
            Set(kp3.publicKey)
          )
          .get
      )
      rcm3 <- buildTestConnectionManager[Task, Secp256k1Key, TestMessage](
        nodeKeyPair = kp3.keyPair,
        clusterConfig = ClusterConfig
          .buildConfig(Set(rcm1.getLocalInfo, rcm2.getLocalInfo), Set())
          .get
      )
      _ <- Resource.liftF[Task, Unit](
        waitFor3Managers((rcm1, 2), (rcm2, 2), (rcm3, 2))(timeOut).void
      )
    } yield (rcm1, rcm2, rcm3))
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

  case class Secp256k1Key(key: BitVector)

  case class NodeInfo(keyPair: AsymmetricCipherKeyPair, publicKey: Secp256k1Key)

  object NodeInfo {
    def generateRandom: NodeInfo = {
      val keyPair = CryptoUtils.generateSecp256k1KeyPair(secureRandom)
      NodeInfo(
        keyPair,
        Secp256k1Key(CryptoUtils.secp256k1KeyPairToNodeId(keyPair))
      )
    }
  }

  object Secp256k1Key {
    implicit val codec: Codec[Secp256k1Key] = bits.as[Secp256k1Key]
  }

  def buildTestConnectionManager[F[
      _
  ]: Concurrent: TaskLift: TaskLike: Timer, K: Codec, M: Codec](
      bindAddress: InetSocketAddress = randomAddress(),
      nodeKeyPair: AsymmetricCipherKeyPair =
        CryptoUtils.generateSecp256k1KeyPair(secureRandom),
      secureRandom: SecureRandom = secureRandom,
      useNativeTlsImplementation: Boolean = false,
      framingConfig: FramingConfig = standardFraming,
      maxIncomingQueueSizePerPeer: Int = testIncomingQueueSize,
      clusterConfig: ClusterConfig[K] = ClusterConfig.empty[K],
      retryConfig: RetryConfig = RetryConfig.default
  )(implicit
      s: Scheduler,
      cs: ContextShift[F]
  ): Resource[F, RemoteConnectionManager[F, K, M]] = {
    ScalanetConnectionProvider
      .scalanetProvider[F, K, M](
        bindAddress,
        nodeKeyPair,
        secureRandom,
        useNativeTlsImplementation,
        framingConfig,
        maxIncomingQueueSizePerPeer
      )
      .flatMap(prov =>
        RemoteConnectionManager(prov, clusterConfig, retryConfig)
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
