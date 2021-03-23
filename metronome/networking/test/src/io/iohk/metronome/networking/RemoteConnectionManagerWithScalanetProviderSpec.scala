package io.iohk.metronome.networking

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, Resource, Timer}
import io.iohk.metronome.networking.RemoteConnectionManager.{
  ClusterConfig,
  MessageReceived,
  RetryConfig
}
import io.iohk.metronome.networking.RemoteConnectionManagerTestUtils._
import io.iohk.metronome.networking.RemoteConnectionManagerWithScalanetProviderSpec.{
  Cluster,
  buildTestConnectionManager,
  secureRandom
}
import io.iohk.scalanet.peergroup.dynamictls.DynamicTLSPeerGroup.FramingConfig
import monix.eval.{Task, TaskLift, TaskLike}
import monix.execution.Scheduler
import org.bouncycastle.crypto.AsymmetricCipherKeyPair
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers
import scodec.Codec

import java.net.InetSocketAddress
import java.security.SecureRandom
import scala.concurrent.duration._

class RemoteConnectionManagerWithScalanetProviderSpec
    extends AsyncFlatSpecLike
    with Matchers {
  implicit val testScheduler =
    Scheduler.fixedPool("RemoteConnectionManagerSpec", 16)

  implicit val timeOut = 10.seconds

  behavior of "RemoteConnectionManagerWithScalanetProvider"

  it should "start connectionManager without any connections" in customTestCaseResourceT(
    buildTestConnectionManager[Task, Secp256k1Key, TestMessage]()
  ) { connectionManager =>
    for {
      connections <- connectionManager.getAcquiredConnections
    } yield assert(connections.isEmpty)
  }

  it should "build fully connected cluster of 3 nodes" in customTestCaseResourceT(
    Cluster.buildCluster(3)
  ) { cluster =>
    for {
      size          <- cluster.clusterSize
      eachNodeCount <- cluster.getEachNodeConnectionsCount
    } yield {
      assert(eachNodeCount.forall(count => count == 2))
      assert(size == 3)
    }
  }

  it should "build fully connected cluster of 4 nodes" in customTestCaseResourceT(
    Cluster.buildCluster(4)
  ) { cluster =>
    for {
      size          <- cluster.clusterSize
      eachNodeCount <- cluster.getEachNodeConnectionsCount
    } yield {
      assert(eachNodeCount.forall(count => count == 3))
      assert(size == 4)
    }
  }

  it should "send and receive messages with other nodes in cluster" in customTestCaseResourceT(
    Cluster.buildCluster(3)
  ) { cluster =>
    for {
      eachNodeCount <- cluster.getEachNodeConnectionsCount
      sendResult    <- cluster.sendMessageFromRandomNodeToAllOthers(MessageA(1))
      (sender, receivers) = sendResult
      received <- Task.traverse(receivers.toList)(receiver =>
        cluster.getMessageFromNode(receiver)
      )
    } yield {
      assert(eachNodeCount.forall(count => count == 2))
      assert(receivers.size == 2)
      assert(received.size == 2)
      //every node should have received the same message
      assert(
        received.forall(receivedMessage =>
          receivedMessage == MessageReceived(sender, MessageA(1))
        )
      )
    }
  }

  it should "eventually connect to previously offline peer" in customTestCaseT {
    val kp1        = NodeInfo.generateRandom(secureRandom)
    val kp2        = NodeInfo.generateRandom(secureRandom)
    val cm2Address = randomAddress()
    for {
      cm1 <- buildTestConnectionManager[Task, Secp256k1Key, TestMessage](
        nodeKeyPair = kp1.keyPair,
        clusterConfig =
          ClusterConfig(clusterNodes = Set((kp2.publicKey, cm2Address)))
      ).allocated
      (cm1Manager, cm1Release) = cm1
      _ <- Task.sleep(5.seconds)
      cm2 <- buildTestConnectionManager[Task, Secp256k1Key, TestMessage](
        bindAddress = cm2Address,
        nodeKeyPair = kp2.keyPair,
        clusterConfig = ClusterConfig(clusterNodes =
          Set((kp1.publicKey, cm1._1.getLocalInfo._2))
        )
      ).allocated
      (cm2Manager, cm2Release) = cm2
      m1HasTheSameNumOfPeersAsM2 <- Task
        .parMap2(
          cm1Manager.getAcquiredConnections,
          cm2Manager.getAcquiredConnections
        ) { case (m1Peers, m2peers) =>
          m1Peers.size == m2peers.size
        }
        .restartUntil(result => result)
        .timeout(10.seconds)
      _ <- Task.parZip2(cm1Release, cm2Release).void
    } yield {
      assert(m1HasTheSameNumOfPeersAsM2)
    }
  }

}
object RemoteConnectionManagerWithScalanetProviderSpec {
  val secureRandom = new SecureRandom()
  val standardFraming =
    FramingConfig.buildStandardFrameConfig(1000000, 4).getOrElse(null)
  val testIncomingQueueSize = 20

  def buildTestConnectionManager[
      F[_]: Concurrent: TaskLift: TaskLike: Timer,
      K: Codec,
      M: Codec
  ](
      bindAddress: InetSocketAddress = randomAddress(),
      nodeKeyPair: AsymmetricCipherKeyPair =
        CryptoUtils.generateSecp256k1KeyPair(secureRandom),
      secureRandom: SecureRandom = secureRandom,
      useNativeTlsImplementation: Boolean = false,
      framingConfig: FramingConfig = standardFraming,
      maxIncomingQueueSizePerPeer: Int = testIncomingQueueSize,
      clusterConfig: ClusterConfig[K] = ClusterConfig(
        Set.empty[(K, InetSocketAddress)]
      ),
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

  type ClusterNodes = Map[
    Secp256k1Key,
    (RemoteConnectionManager[Task, Secp256k1Key, TestMessage], Task[Unit])
  ]

  def buildClusterNodes(
      keys: NonEmptyList[NodeInfo]
  )(implicit
      s: Scheduler,
      timeOut: FiniteDuration
  ): Task[Ref[Task, ClusterNodes]] = {
    val keyWithAddress = keys.toList.map(key => (key, randomAddress())).toSet

    for {
      nodes <- Ref.of[Task, ClusterNodes](Map.empty)
      _ <- Task.traverse(keyWithAddress) { case (info, address) =>
        buildTestConnectionManager[Task, Secp256k1Key, TestMessage](
          bindAddress = address,
          nodeKeyPair = info.keyPair,
          clusterConfig = ClusterConfig(clusterNodes =
            keyWithAddress.map(keyWithAddress =>
              (keyWithAddress._1.publicKey, keyWithAddress._2)
            )
          )
        ).allocated.flatMap { case (manager, release) =>
          nodes.update(map =>
            map + (manager.getLocalInfo._1 -> (manager, release))
          )
        }
      }

    } yield nodes
  }

  class Cluster(nodes: Ref[Task, ClusterNodes]) {

    def clusterSize: Task[Int] = nodes.get.map(_.size)

    def getEachNodeConnectionsCount: Task[List[Int]] = {
      for {
        runningNodes <- nodes.get.flatMap(nodes =>
          Task.traverse(nodes.values.map(_._1))(manager =>
            manager.getAcquiredConnections
          )
        )

      } yield runningNodes.map(_.size).toList
    }

    def closeAllNodes: Task[Unit] = {
      nodes.get.flatMap { nodes =>
        Task
          .parTraverseUnordered(nodes.values) { case (node, release) =>
            release
          }
          .void
      }
    }

    def sendMessageFromRandomNodeToAllOthers(
        message: TestMessage
    ): Task[(Secp256k1Key, Set[Secp256k1Key])] = {
      for {
        runningNodes <- nodes.get
        (key, (node, _)) = runningNodes.head
        nodesRececivingMessage <- node.getAcquiredConnections.flatMap {
          connections =>
            Task
              .traverse(connections)(connectionKey =>
                node.sendMessage(connectionKey, message)
              )
              .map(_ => connections)
        }
      } yield (key, nodesRececivingMessage)
    }

    def getMessageFromNode(key: Secp256k1Key) = {
      nodes.get.flatMap { runningNodes =>
        runningNodes(key)._1.incomingMessages.take(1).toListL.map(_.head)
      }
    }

  }

  object Cluster {
    def buildCluster(size: Int)(implicit
        s: Scheduler,
        timeOut: FiniteDuration
    ): Resource[Task, Cluster] = {
      val nodeInfos = NonEmptyList.fromListUnsafe(
        ((0 until size).map(_ => NodeInfo.generateRandom(secureRandom)).toList)
      )

      Resource.make {
        for {
          nodes <- buildClusterNodes(nodeInfos)
          cluster = new Cluster(nodes)
          _ <- cluster.getEachNodeConnectionsCount
            .restartUntil(counts => counts.forall(count => count == size - 1))
            .timeout(timeOut)
        } yield cluster
      } { cluster => cluster.closeAllNodes }
    }

  }

}
