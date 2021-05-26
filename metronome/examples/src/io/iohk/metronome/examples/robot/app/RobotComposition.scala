package io.iohk.metronome.examples.robot.app

import cats.implicits._
import cats.effect.{Resource, Concurrent, Blocker}
import monix.eval.Task
import monix.execution.Scheduler
import io.iohk.metronome.crypto.{ECKeyPair, ECPublicKey, GroupSignature}
import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.hotstuff.service.tracing.{
  ConsensusTracers,
  SyncTracers
}
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
import io.iohk.metronome.hotstuff.service.{HotStuffService, ConsensusService}
import io.iohk.metronome.hotstuff.service.messages.{
  DuplexMessage,
  HotStuffMessage
}
import io.iohk.metronome.hotstuff.service.storage.{
  BlockStorage,
  ViewStateStorage
}
import io.iohk.metronome.networking.{
  EncryptedConnectionProvider,
  ScalanetConnectionProvider,
  RemoteConnectionManager,
  NetworkTracers,
  Network
}
import io.iohk.metronome.examples.robot.RobotAgreement
import io.iohk.metronome.examples.robot.codecs.RobotCodecs
import io.iohk.metronome.examples.robot.models.{RobotBlock, Robot, RobotSigning}
import io.iohk.metronome.examples.robot.service.RobotService
import io.iohk.metronome.examples.robot.service.messages.RobotMessage
import io.iohk.metronome.examples.robot.app.config.{RobotConfig, RobotOptions}
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
  KVRingBuffer,
  KVTree
}
import io.iohk.scalanet.peergroup.dynamictls.DynamicTLSPeerGroup
import java.security.SecureRandom
import scodec.Codec
import scodec.bits.ByteVector
import java.nio.file.Files

/** Composition root for dependency injection.
  *
  * We can subclass it in integration tests to provide mocks where appropriate.
  */
trait RobotComposition {
  import RobotCodecs._

  type NetworkMessage =
    DuplexMessage[RobotAgreement, RobotMessage]

  type ConnectionManager =
    RemoteConnectionManager[Task, ECPublicKey, NetworkMessage]

  type NS = RocksDBStore.Namespace

  type NTS = NetworkTracers[Task, ECPublicKey, NetworkMessage]
  type CTS = ConsensusTracers[Task, RobotAgreement]
  type STS = SyncTracers[Task, RobotAgreement]

  /** Storages to be returned so we can look at state in tests. */
  case class Storages(
      blockStorage: BlockStorage[NS, RobotAgreement],
      viewStateStorage: ViewStateStorage[NS, RobotAgreement],
      stateStorage: KVRingBuffer[NS, Hash, Robot.State]
  )(implicit val storeRunner: KVStoreRunner[Task, NS])

  def compose(
      opts: RobotOptions,
      config: RobotConfig
  ): Resource[Task, Storages] = {

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
      height = 0,
      postStateHash = genesisState.hash,
      command = Robot.Command.Rest
    )

    implicit val networkTracers: NTS  = makeNetworkTracers
    implicit val consesusTracers: CTS = makeConsensusTracers
    implicit val syncTracers: STS     = makeSyncTracers

    for {

      connectionManager <- makeConnectionManager(config, opts)

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

    } yield Storages(blockStorage, viewStateStorage, stateStorage)
  }

  protected def makeNetworkTracers =
    RobotNetworkTracers.networkHybridLogTracers

  protected def makeConsensusTracers =
    RobotConsensusTracers.consensusHybridLogTracers

  protected def makeSyncTracers =
    RobotSyncTracers.syncHybridLogTracers

  protected def makeConnectionProvider(
      config: RobotConfig,
      opts: RobotOptions
  ) = {
    val localNode = config.network.nodes(opts.nodeIndex)
    for {
      implicit0(scheduler: Scheduler) <- Resource.make(
        Task(Scheduler.io("scalanet"))
      )(scheduler => Task(scheduler.shutdown()))

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
    } yield connectionProvider
  }

  protected def makeConnectionManager(
      config: RobotConfig,
      connectionProvider: EncryptedConnectionProvider[
        Task,
        ECPublicKey,
        NetworkMessage
      ]
  )(implicit
      networkTracers: NTS
  ): Resource[Task, ConnectionManager] = {

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

  protected def makeConnectionManager(
      config: RobotConfig,
      opts: RobotOptions
  )(implicit
      networkTracers: NTS
  ): Resource[Task, ConnectionManager] =
    for {
      connectionProvider <- makeConnectionProvider(config, opts)
      connectionManager  <- makeConnectionManager(config, connectionProvider)
    } yield connectionManager

  protected def makeRocksDBStore(
      config: RobotConfig,
      opts: RobotOptions
  ) = {
    val dbConfig = RocksDBStore.Config.default(
      config.db.path.resolve(opts.nodeIndex.toString)
    )
    for {
      dir <- Resource.liftF {
        Task {
          Files.createDirectories(dbConfig.path)
        }
      }
      blocker <- makeDBBlocker
      db      <- RocksDBStore[Task](dbConfig, RobotNamespaces.all, blocker)
    } yield db
  }

  protected def makeDBBlocker =
    Blocker[Task]

  protected def makeKVStoreRunner(
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

  protected def makeNetworks(
      connectionManager: RemoteConnectionManager[
        Task,
        ECPublicKey,
        NetworkMessage
      ]
  ) = {
    val network = Network
      .fromRemoteConnnectionManager[Task, RobotAgreement.PKey, NetworkMessage](
        connectionManager
      )

    for {
      (hotstuffNetwork, applicationNetwork) <- Network.splitter[
        Task,
        RobotAgreement.PKey,
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

  protected def makeBlockStorage(genesis: RobotBlock)(implicit
      storeRunner: KVStoreRunner[Task, NS]
  ) = {
    implicit def `Codec[Set[T]]`[T: Codec] = {
      import scodec.codecs.implicits._
      Codec[List[T]].xmap[Set[T]](_.toSet, _.toList)
    }

    val blockStorage = new BlockStorage[NS, RobotAgreement](
      blockColl = new KVCollection[NS, Hash, RobotBlock](RobotNamespaces.Block),
      blockMetaColl = new KVCollection[NS, Hash, KVTree.NodeMeta[Hash]](
        RobotNamespaces.BlockMeta
      ),
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

  protected def makeViewStateStorage(genesis: RobotBlock)(implicit
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

  protected def makeStateStorage(
      config: RobotConfig,
      genesisState: Robot.State
  )(implicit
      storeRunner: KVStoreRunner[Task, NS]
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

  protected def makeApplicationService(
      config: RobotConfig,
      opts: RobotOptions,
      applicationNetwork: Network[Task, RobotAgreement.PKey, RobotMessage],
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

  protected def makeHotstuffService(
      config: RobotConfig,
      opts: RobotOptions,
      genesis: RobotBlock,
      hotstuffNetwork: Network[
        Task,
        RobotAgreement.PKey,
        HotStuffMessage[RobotAgreement]
      ],
      appService: RobotService[Task, NS],
      blockStorage: BlockStorage[NS, RobotAgreement],
      viewStateStorage: ViewStateStorage[NS, RobotAgreement]
  )(implicit
      storeRunner: KVStoreRunner[Task, NS],
      consensusTracers: CTS,
      syncTracers: STS
  ) = {
    // Round-Robin is more predictable than Hashing.
    implicit val leaderSelection = LeaderSelection.RoundRobin
    implicit val signing         = new RobotSigning(genesis.hash)

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
            bundle        <- viewStateStorage.getBundle
            maybePrepared <- blockStorage.get(bundle.prepareQC.blockHash)
            prepared = maybePrepared.getOrElse {
              throw new IllegalStateException(
                s"Cannot get the last prepared block from storage."
              )
            }
          } yield (bundle, prepared)
        }
      }

      // Start from the next view number, so we aren't in Prepare state when it was, say, PreCommit before.
      protocolState = ProtocolState[RobotAgreement](
        viewNumber = viewState.viewNumber.next,
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

  protected def makeBlockPruner(
      config: RobotConfig,
      blockStorage: BlockStorage[NS, RobotAgreement],
      viewStateStorage: ViewStateStorage[NS, RobotAgreement]
  )(implicit storeRunner: KVStoreRunner[Task, NS]) =
    Concurrent[Task].background {
      val query: KVStore[NS, Unit] = for {
        // Always keep the last executed block.
        lastExecutedBlock <- viewStateStorage.getLastExecutedBlockHash.lift
        pathFromRoot      <- blockStorage.getPathFromRoot(lastExecutedBlock).lift

        // Keep the last N blocks.
        pruneable = pathFromRoot.reverse
          .drop(config.db.blockHistorySize)
          .reverse

        // Make the last pruneable block the new root.
        _ <- pruneable.lastOption match {
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

object RobotComposition extends RobotComposition
