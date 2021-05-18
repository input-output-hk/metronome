package io.iohk.metronome.examples.robot.app

import cats.implicits._
import cats.effect.{ExitCode, Resource, Concurrent}
import monix.eval.{Task, TaskApp}
import monix.execution.Scheduler
import io.iohk.metronome.crypto.{ECKeyPair, ECPublicKey, GroupSignature}
import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.hotstuff.consensus.{
  Federation,
  LeaderSelection,
  ViewNumber
}
import io.iohk.metronome.hotstuff.consensus.basic.{
  QuorumCertificate,
  Phase,
  ProtocolState
}
import io.iohk.metronome.hotstuff.service.{
  Network,
  HotStuffService,
  ConsensusService
}
import io.iohk.metronome.hotstuff.service.messages.{
  DuplexMessage,
  HotStuffMessage
}
import io.iohk.metronome.networking.{
  EncryptedConnectionProvider,
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
import io.iohk.metronome.examples.robot.app.tracing.{
  RobotNetworkTracers,
  RobotConsensusTracers,
  RobotSyncTracers
}
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
import scodec.bits.ByteVector
import java.nio.file.Files

object RobotApp extends TaskApp {
  import RobotCodecs._

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
            setLogProperties(opts) >>
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

  def setLogProperties(opts: CommandLineOptions): Task[Unit] = Task {
    // Separate log file for each node.
    System.setProperty("log.file.name", s"robot/logs/node-${opts.nodeIndex}")
    // Not logging to the console so we can display robot position.
    System.setProperty("log.console.level", s"INFO")
  }.void

  def compose(
      opts: CommandLineOptions,
      config: RobotConfig
  ): Resource[Task, Unit] = {

    val genesisState = Robot
      .State(
        position = Robot.Position(
          row = config.model.maxRow / 2,
          col = config.model.maxCol / 2
        ),
        orientation = Robot.Orientation.North
      )

    val genesis = RobotBlock(
      parentHash = Hash(ByteVector.empty),
      postStateHash = genesisState.hash,
      command = Robot.Command.Rest
    )

    for {
      connectionProvider <- makeConnectionProvider(config, opts)
      connectionManager  <- makeConnectionManager(config, connectionProvider)

      (hotstuffNetwork, applicationNetwork) <- makeNetworks(connectionManager)

      db <- makeRocksDBStore(config, opts)
      implicit0(storeRunner: KVStoreRunner[Task, NS]) = makeKVStoreRunner(db)

      blockStorage     <- makeBlockStorage(genesis)
      viewStateStorage <- makeViewStateStorage(genesis)
      stateStorage     <- makeStateStorage(config, genesisState)

      appService <- makeApplicationService(
        config,
        opts,
        applicationNetwork,
        blockStorage,
        viewStateStorage,
        stateStorage
      )

      _ <- makeHotstuffService(
        config,
        opts,
        genesis,
        hotstuffNetwork,
        appService,
        blockStorage,
        viewStateStorage
      )

      _ <- makeBlockPruner(config, blockStorage, viewStateStorage)

    } yield ()
  }

  private def makeConnectionProvider(
      config: RobotConfig,
      opts: CommandLineOptions
  ) = {
    implicit val scheduler = Scheduler.io("scalanet")

    val localNode = config.network.nodes(opts.nodeIndex)

    ScalanetConnectionProvider[
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
  }

  private def makeConnectionManager(
      config: RobotConfig,
      connectionProvider: EncryptedConnectionProvider[
        Task,
        ECPublicKey,
        NetworkMessage
      ]
  ) = {
    import RobotNetworkTracers.networkTracers

    val clusterConfig = RemoteConnectionManager.ClusterConfig(
      clusterNodes = config.network.nodes.map { node =>
        node.publicKey -> node.address
      }.toSet
    )
    val retryConfig = RemoteConnectionManager.RetryConfig.default

    RemoteConnectionManager[
      Task,
      ECPublicKey,
      NetworkMessage
    ](connectionProvider, clusterConfig, retryConfig)
  }

  private def makeRocksDBStore(
      config: RobotConfig,
      opts: CommandLineOptions
  ) = {
    val dbConfig = RocksDBStore.Config.default(
      config.db.path.resolve(opts.nodeIndex.toString)
    )
    Resource.liftF {
      Task {
        Files.createDirectories(dbConfig.path)
      }
    } >>
      RocksDBStore[Task](dbConfig, RobotNamespaces.all)
  }

  private def makeKVStoreRunner(
      db: RocksDBStore[Task]
  ) = {
    new KVStoreRunner[Task, NS] {
      override def runReadOnly[A](
          query: KVStoreRead[NS, A]
      ): Task[A] = db.runReadOnly(query)

      override def runReadWrite[A](query: KVStore[NS, A]): Task[A] =
        db.runWithBatching(query)
    }
  }

  private def makeNetworks(
      connectionManager: RemoteConnectionManager[
        Task,
        ECPublicKey,
        NetworkMessage
      ]
  ) = {
    val network = Network
      .fromRemoteConnnectionManager[Task, RobotAgreement, NetworkMessage](
        connectionManager
      )

    for {
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
    } yield (hotstuffNetwork, applicationNetwork)
  }

  private def makeBlockStorage(genesis: RobotBlock)(implicit
      storeRunner: KVStoreRunner[Task, NS]
  ) = {
    val blockStorage = new BlockStorage[NS, RobotAgreement](
      blockColl = new KVCollection[NS, Hash, RobotBlock](RobotNamespaces.Block),
      childToParentColl =
        new KVCollection[NS, Hash, Hash](RobotNamespaces.BlockToParent),
      parentToChildrenColl =
        new KVCollection[NS, Hash, Set[Hash]](RobotNamespaces.BlockToChildren)
    )

    // (Re)insert genesis. It's okay if it has been pruned before,
    // but if the application is just starting it will need it.
    Resource
      .liftF {
        storeRunner.runReadWrite {
          blockStorage.put(genesis)
        }
      }
      .as(blockStorage)
  }

  private def makeViewStateStorage(genesis: RobotBlock)(implicit
      storeRunner: KVStoreRunner[Task, NS]
  ) = Resource.liftF {
    val genesisQC = QuorumCertificate[RobotAgreement](
      phase = Phase.Prepare,
      viewNumber = ViewNumber(0),
      blockHash = genesis.hash,
      signature = GroupSignature(Nil)
    )
    storeRunner.runReadWrite {
      ViewStateStorage[NS, RobotAgreement](
        RobotNamespaces.ViewState,
        genesis = ViewStateStorage.Bundle.fromGenesisQC(genesisQC)
      )
    }
  }

  private def makeStateStorage(config: RobotConfig, genesisState: Robot.State)(
      implicit storeRunner: KVStoreRunner[Task, NS]
  ) = Resource.liftF {
    for {
      coll <- Task.pure {
        new KVCollection[NS, Hash, Robot.State](RobotNamespaces.State)
      }
      // Insert the genesis state straight into the underlying collection,
      // not the ringbuffer, so it doesn't get evicted if we restart the
      // app a few times.
      _ <- storeRunner.runReadWrite {
        coll.put(genesisState.hash, genesisState)
      }
      stateStorage =
        new KVRingBuffer[NS, Hash, Robot.State](
          coll,
          metaNamespace = RobotNamespaces.StateMeta,
          maxHistorySize = config.db.stateHistorySize
        )
    } yield stateStorage
  }

  private def makeApplicationService(
      config: RobotConfig,
      opts: CommandLineOptions,
      applicationNetwork: Network[Task, RobotAgreement, RobotMessage],
      blockStorage: BlockStorage[NS, RobotAgreement],
      viewStateStorage: ViewStateStorage[NS, RobotAgreement],
      stateStorage: KVRingBuffer[NS, Hash, Robot.State]
  )(implicit
      storeRunner: KVStoreRunner[Task, NS]
  ) =
    RobotService[Task, NS](
      maxRow = config.model.maxRow,
      maxCol = config.model.maxCol,
      publicKey = config.network.nodes(opts.nodeIndex).publicKey,
      network = applicationNetwork,
      blockStorage = blockStorage,
      viewStateStorage = viewStateStorage,
      stateStorage = stateStorage,
      simulatedDecisionTime = config.model.simulatedDecisionTime,
      timeout = config.network.timeout
    )

  private def makeHotstuffService(
      config: RobotConfig,
      opts: CommandLineOptions,
      genesis: RobotBlock,
      hotstuffNetwork: Network[
        Task,
        RobotAgreement,
        HotStuffMessage[RobotAgreement]
      ],
      appService: RobotService[Task, NS],
      blockStorage: BlockStorage[NS, RobotAgreement],
      viewStateStorage: ViewStateStorage[NS, RobotAgreement]
  )(implicit
      storeRunner: KVStoreRunner[Task, NS]
  ) = {
    import RobotConsensusTracers._
    import RobotSyncTracers._

    implicit val leaderSelection = LeaderSelection.Hashing

    implicit val signing = new RobotSigning(genesis.hash)

    val localNode = config.network.nodes(opts.nodeIndex)

    for {
      federation <- Resource.liftF {
        Task.fromEither((e: String) => new IllegalArgumentException(e))(
          Federation(config.network.nodes.map(_.publicKey).toVector)
        )
      }

      (viewState, preparedBlock) <- Resource.liftF {
        storeRunner.runReadOnly {
          for {
            bundle   <- viewStateStorage.getBundle
            prepared <- blockStorage.get(bundle.prepareQC.blockHash)
          } yield (bundle, prepared.get)
        }
      }

      protocolState = ProtocolState[RobotAgreement](
        viewNumber = viewState.viewNumber,
        phase = Phase.Prepare,
        publicKey = localNode.publicKey,
        signingKey = localNode.privateKey,
        federation = federation,
        prepareQC = viewState.prepareQC,
        lockedQC = viewState.lockedQC,
        commitQC = viewState.commitQC,
        preparedBlock = preparedBlock,
        timeout = config.consensus.minTimeout,
        votes = Set.empty,
        newViews = Map.empty
      )

      _ <- HotStuffService[Task, NS, RobotAgreement](
        publicKey = localNode.publicKey,
        hotstuffNetwork,
        appService,
        blockStorage,
        viewStateStorage,
        protocolState,
        consensusConfig = ConsensusService.Config(
          timeoutPolicy = ConsensusService.TimeoutPolicy.exponential(
            factor = config.consensus.timeoutFactor,
            minTimeout = config.consensus.minTimeout,
            maxTimeout = config.consensus.maxTimeout
          )
        )
      )
    } yield ()
  }

  def makeBlockPruner(
      config: RobotConfig,
      blockStorage: BlockStorage[NS, RobotAgreement],
      viewStateStorage: ViewStateStorage[NS, RobotAgreement]
  )(implicit storeRunner: KVStoreRunner[Task, NS]) =
    Concurrent[Task].background {
      val query: KVStore[NS, Unit] = for {
        lastExecutedBlock <- viewStateStorage.getLastExecutedBlockHash.lift
        pathFromRoot      <- blockStorage.getPathFromRoot(lastExecutedBlock).lift

        pruneable = pathFromRoot.reverse
          .drop(config.db.blockHistorySize)
          .reverse

        _ <- pruneable.headOption match {
          case Some(newRoot) =>
            blockStorage.pruneNonDescendants(newRoot) >>
              viewStateStorage.setRootBlockHash(newRoot)

          case None =>
            KVStore.instance[NS].unit
        }
      } yield ()

      storeRunner
        .runReadWrite(query)
        .delayResult(config.db.pruneInterval)
        .foreverM
    }
}
