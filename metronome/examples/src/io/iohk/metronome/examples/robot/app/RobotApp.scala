package io.iohk.metronome.examples.robot.app

import cats.effect.{ExitCode, Resource}
import monix.eval.{Task, TaskApp}
import io.iohk.metronome.crypto.{ECKeyPair, ECPublicKey, GroupSignature}
import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.hotstuff.consensus.{
  Federation,
  LeaderSelection,
  ViewNumber
}
import io.iohk.metronome.hotstuff.consensus.basic.{QuorumCertificate, Phase}
import io.iohk.metronome.hotstuff.service.Network
import io.iohk.metronome.hotstuff.service.messages.{
  DuplexMessage,
  HotStuffMessage
}
import io.iohk.metronome.networking.{
  ScalanetConnectionProvider,
  RemoteConnectionManager
}
import io.iohk.metronome.hotstuff.service.storage.{
  BlockStorage,
  ViewStateStorage
}
import io.iohk.metronome.examples.robot.RobotAgreement
import io.iohk.metronome.examples.robot.codecs.RobotCodecs
import io.iohk.metronome.examples.robot.models.{RobotBlock, Robot, RobotSigning}
import io.iohk.metronome.examples.robot.service.RobotService
import io.iohk.metronome.examples.robot.service.messages.RobotMessage
import io.iohk.metronome.examples.robot.app.config.{
  RobotConfigParser,
  RobotConfig
}
import io.iohk.metronome.examples.robot.app.tracing.RobotNetworkTracers
import io.iohk.metronome.rocksdb.RocksDBStore
import io.iohk.metronome.storage.{
  KVStoreRunner,
  KVStoreRead,
  KVStore,
  KVCollection,
  KVRingBuffer
}
import io.iohk.scalanet.peergroup.dynamictls.DynamicTLSPeerGroup
import java.security.SecureRandom
import scopt.OParser
import scodec.Codec

object RobotApp extends TaskApp {
  type NetworkMessage = DuplexMessage[RobotAgreement, RobotMessage]

  type NS = RocksDBStore.Namespace

  case class CommandLineOptions(
      nodeIndex: Int = 0
  )

  def oparser(config: RobotConfig) = {
    val builder = OParser.builder[CommandLineOptions]
    import builder._

    OParser.sequence(
      programName("robot"),
      opt[Int]('i', "node-index")
        .action((i, opts) => opts.copy(nodeIndex = i))
        .text("index of example node to run")
        .required()
        .validate(i =>
          Either.cond(
            0 <= i && i < config.network.nodes.length,
            (),
            s"Must be between 0 and ${config.network.nodes.length - 1}"
          )
        )
    )
  }

  override def run(args: List[String]): Task[ExitCode] = {
    RobotConfigParser.parse match {
      case Left(error) =>
        Task.delay(println(error)).as(ExitCode.Error)
      case Right(config) =>
        OParser.parse(oparser(config), args, CommandLineOptions()) match {
          case None =>
            Task.pure(ExitCode.Error)
          case Some(opts) =>
            run(opts, config)
        }
    }
  }

  def run(opts: CommandLineOptions, config: RobotConfig): Task[ExitCode] =
    compose(opts, config).use(_ => Task.never.as(ExitCode.Success))

  implicit def `Codec[Set[T]]`[T: Codec] = {
    import scodec.codecs.implicits._
    Codec[List[T]].xmap[Set[T]](_.toSet, _.toList)
  }

  def compose(
      opts: CommandLineOptions,
      config: RobotConfig
  ): Resource[Task, Unit] = {
    import RobotCodecs._
    import RobotNetworkTracers.networkTracers
    implicit val scheduler = this.scheduler

    val federation =
      Federation(config.network.nodes.map(_.publicKey).toVector)(
        LeaderSelection.Hashing
      )

    val localNode = config.network.nodes(opts.nodeIndex)

    val dbConfig =
      RocksDBStore.Config.default(
        config.db.path.resolve(opts.nodeIndex.toString)
      )

    val clusterConfig = RemoteConnectionManager.ClusterConfig(
      clusterNodes = config.network.nodes.map { node =>
        node.publicKey -> node.address
      }.toSet
    )
    val retryConfig = RemoteConnectionManager.RetryConfig.default

    val genesis = RobotBlock.genesis(
      row = config.model.maxRow / 2,
      col = config.model.maxCol / 2,
      orientation = Robot.Orientation.North
    )

    val genesisQC = QuorumCertificate[RobotAgreement](
      phase = Phase.Prepare,
      viewNumber = ViewNumber(0),
      blockHash = genesis.hash,
      signature = GroupSignature(Nil)
    )

    implicit val signing = new RobotSigning(genesis.hash)

    for {
      connectionProvider <- ScalanetConnectionProvider[
        Task,
        ECPublicKey,
        NetworkMessage
      ](
        bindAddress = localNode.address,
        nodeKeyPair = ECKeyPair(localNode.privateKey, localNode.publicKey),
        new SecureRandom(),
        useNativeTlsImplementation = true,
        framingConfig = DynamicTLSPeerGroup.FramingConfig
          .buildStandardFrameConfig(
            maxFrameLength = 1024 * 1024,
            lengthFieldLength = 8
          )
          .fold(e => sys.error(e.description), identity),
        maxIncomingQueueSizePerPeer = 100
      )

      connectionManager <- RemoteConnectionManager[
        Task,
        ECPublicKey,
        NetworkMessage
      ](connectionProvider, clusterConfig, retryConfig)

      rocksDbStore <- RocksDBStore[Task](dbConfig, RobotNamespaces.all)

      implicit0(storeRunner: KVStoreRunner[Task, NS]) =
        new KVStoreRunner[Task, NS] {
          override def runReadOnly[A](
              query: KVStoreRead[NS, A]
          ): Task[A] = rocksDbStore.runReadOnly(query)

          override def runReadWrite[A](query: KVStore[NS, A]): Task[A] =
            rocksDbStore.runWithBatching(query)
        }

      network = Network
        .fromRemoteConnnectionManager[Task, RobotAgreement, NetworkMessage](
          connectionManager
        )

      (hotstuffNetwork, applicationNetwork) <- Network.splitter[
        Task,
        RobotAgreement,
        NetworkMessage,
        HotStuffMessage[RobotAgreement],
        RobotMessage
      ](network)(
        split = {
          case DuplexMessage.AgreementMessage(m)   => Left(m)
          case DuplexMessage.ApplicationMessage(m) => Right(m)
        },
        merge = {
          case Left(m)  => DuplexMessage.AgreementMessage(m)
          case Right(m) => DuplexMessage.ApplicationMessage(m)
        }
      )

      blockStorage = new BlockStorage[NS, RobotAgreement](
        blockColl =
          new KVCollection[NS, Hash, RobotBlock](RobotNamespaces.Block),
        childToParentColl =
          new KVCollection[NS, Hash, Hash](RobotNamespaces.BlockToParent),
        parentToChildrenColl =
          new KVCollection[NS, Hash, Set[Hash]](RobotNamespaces.BlockToChildren)
      )

      // (Re)insert genesis. It's okay if it has been pruned before,
      // but if the application is just starting it will need it.
      _ <- Resource.liftF {
        storeRunner.runReadWrite {
          blockStorage.put(genesis)
        }
      }

      viewStateStorage <- Resource.liftF {
        storeRunner.runReadWrite {
          ViewStateStorage[NS, RobotAgreement](
            RobotNamespaces.ViewState,
            genesis = ViewStateStorage.Bundle.fromGenesisQC(genesisQC)
          )
        }
      }

      stateStorage = new KVRingBuffer[NS, Hash, Robot.State](
        coll = new KVCollection[NS, Hash, Robot.State](RobotNamespaces.State),
        metaNamespace = RobotNamespaces.StateMeta,
        maxHistorySize = config.db.stateHistorySize
      )

      applicationService <- Resource.liftF {
        RobotService[Task, NS](
          maxRow = config.model.maxRow,
          maxCol = config.model.maxCol,
          network = applicationNetwork,
          blockStorage = blockStorage,
          viewStateStorage = viewStateStorage,
          stateStorage = stateStorage,
          simulatedDecisionTime = config.model.simulatedDecisionTime,
          timeout = config.network.timeout
        )
      }
    } yield ()
  }
}
