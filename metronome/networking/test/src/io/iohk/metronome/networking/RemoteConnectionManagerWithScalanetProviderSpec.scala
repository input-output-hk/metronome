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
  buildTestConnectionManager
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

  it should "eventually reconnect to offline node" in customTestCaseResourceT(
    Cluster.buildCluster(3)
  ) { cluster =>
    for {
      size   <- cluster.clusterSize
      killed <- cluster.shutdownRandomNode
      _      <- cluster.sendMessageFromRandomNodeToAllOthers(MessageA(1))
      (address, keyPair, clusterConfig) = killed
      _ <- cluster.waitUntilEveryNodeHaveNConnections(1)
      // be offline for a moment
      _                      <- Task.sleep(3.seconds)
      connectionAfterFailure <- cluster.getEachNodeConnectionsCount
      _                      <- cluster.startNode(address, keyPair, clusterConfig)
      _                      <- cluster.waitUntilEveryNodeHaveNConnections(2)
    } yield {
      assert(size == 3)
      assert(connectionAfterFailure.forall(connections => connections == 1))
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
      retryConfig: RetryConfig =
        RetryConfig.default.copy(maxRandomJitter = Some(250.milliseconds))
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
    (
        RemoteConnectionManager[Task, Secp256k1Key, TestMessage],
        AsymmetricCipherKeyPair,
        ClusterConfig[Secp256k1Key],
        Task[Unit]
    )
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
        val clusterConfig = ClusterConfig(clusterNodes =
          keyWithAddress.map(keyWithAddress =>
            (keyWithAddress._1.publicKey, keyWithAddress._2)
          )
        )

        buildTestConnectionManager[Task, Secp256k1Key, TestMessage](
          bindAddress = address,
          nodeKeyPair = info.keyPair,
          clusterConfig = clusterConfig
        ).allocated.flatMap { case (manager, release) =>
          nodes.update(map =>
            map + (manager.getLocalInfo._1 -> (manager, info.keyPair, clusterConfig, release))
          )
        }
      }

    } yield nodes
  }

  class Cluster(nodes: Ref[Task, ClusterNodes]) {

    private def broadcastToAllConnections(
        manager: RemoteConnectionManager[Task, Secp256k1Key, TestMessage],
        message: TestMessage
    ) = {
      manager.getAcquiredConnections.flatMap { connections =>
        Task
          .parTraverseUnordered(connections)(connectionKey =>
            manager.sendMessage(connectionKey, message)
          )
          .map { _ =>
            connections
          }
      }

    }

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

    def waitUntilEveryNodeHaveNConnections(
        n: Int
    )(implicit timeOut: FiniteDuration): Task[List[Int]] = {
      getEachNodeConnectionsCount
        .restartUntil(counts =>
          counts.forall(currentNodeConnectionCount =>
            currentNodeConnectionCount == n
          )
        )
        .timeout(timeOut)
    }

    def closeAllNodes: Task[Unit] = {
      nodes.get.flatMap { nodes =>
        Task
          .parTraverseUnordered(nodes.values) { case (node, _, _, release) =>
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
        (key, (node, _, _, _)) = runningNodes.head
        nodesReceivingMessage <- broadcastToAllConnections(node, message)
      } yield (key, nodesReceivingMessage)
    }

    def sendMessageFromAllClusterNodesToTheirConnections(
        message: TestMessage
    ): Task[List[(Secp256k1Key, Set[Secp256k1Key])]] = {
      nodes.get.flatMap { current =>
        Task.parTraverseUnordered(current.values) { case (manager, _, _, _) =>
          broadcastToAllConnections(manager, message).map { receivers =>
            (manager.getLocalInfo._1 -> receivers)
          }
        }
      }
    }

    def getMessageFromNode(key: Secp256k1Key) = {
      nodes.get.flatMap { runningNodes =>
        runningNodes(key)._1.incomingMessages.take(1).toListL.map(_.head)
      }
    }

    def shutdownRandomNode: Task[
      (InetSocketAddress, AsymmetricCipherKeyPair, ClusterConfig[Secp256k1Key])
    ] = {
      for {
        current <- nodes.get
        (
          randomNodeKey,
          (randomManager, nodeKeyPair, clusterConfig, randomRelease)
        ) = current.head
        _ <- randomRelease
        _ <- nodes.update(current => current - randomNodeKey)
      } yield (randomManager.getLocalInfo._2, nodeKeyPair, clusterConfig)
    }

    def startNode(
        bindAddress: InetSocketAddress,
        key: AsymmetricCipherKeyPair,
        clusterConfig: ClusterConfig[Secp256k1Key]
    )(implicit s: Scheduler): Task[Unit] = {
      buildTestConnectionManager[Task, Secp256k1Key, TestMessage](
        bindAddress = bindAddress,
        nodeKeyPair = key,
        clusterConfig = clusterConfig
      ).allocated.flatMap { case (manager, release) =>
        nodes.update { current =>
          current + (manager.getLocalInfo._1 -> (manager, key, clusterConfig, release))
        }
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
