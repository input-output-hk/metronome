package io.iohk.metronome.checkpointing.app

import cats.implicits._
import cats.effect.{Resource, Blocker, Concurrent}
import io.iohk.metronome.crypto.{
  ECKeyPair,
  ECPublicKey,
  ECPrivateKey,
  GroupSignature
}
import io.iohk.metronome.checkpointing.CheckpointingAgreement
import io.iohk.metronome.checkpointing.app.codecs.CheckpointingCodecs
import io.iohk.metronome.checkpointing.app.config.{
  CheckpointingConfig,
  CheckpointingConfigParser
}
import io.iohk.metronome.checkpointing.app.tracing._
import io.iohk.metronome.checkpointing.service.CheckpointingService
import io.iohk.metronome.checkpointing.service.messages.CheckpointingMessage
import io.iohk.metronome.checkpointing.service.tracing.CheckpointingEvent
import io.iohk.metronome.checkpointing.service.storage.LedgerStorage
import io.iohk.metronome.checkpointing.models.{Block, Ledger}
import io.iohk.metronome.checkpointing.interpreter.InterpreterConnection
import io.iohk.metronome.checkpointing.interpreter.messages.InterpreterMessage
import io.iohk.metronome.checkpointing.interpreter.codecs.DefaultInterpreterCodecs
import io.iohk.metronome.hotstuff.consensus.{
  ViewNumber,
  Federation,
  LeaderSelection
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
import io.iohk.metronome.hotstuff.service.tracing.{
  ConsensusTracers,
  SyncTracers
}
import io.iohk.metronome.hotstuff.service.storage.{
  BlockStorage,
  ViewStateStorage,
  BlockPruning
}
import io.iohk.metronome.networking.{
  EncryptedConnectionProvider,
  ScalanetConnectionProvider,
  RemoteConnectionManager,
  LocalConnectionManager,
  NetworkTracers,
  Network
}
import io.iohk.metronome.rocksdb.RocksDBStore
import io.iohk.metronome.storage.{
  KVStoreRunner,
  KVStoreRead,
  KVStore,
  KVCollection,
  KVTree
}
import io.iohk.scalanet.peergroup.dynamictls.DynamicTLSPeerGroup
import io.circe.Json
import java.security.SecureRandom
import java.nio.file.Files
import monix.eval.Task
import monix.execution.Scheduler
import scodec.Codec
import io.iohk.metronome.tracer.Tracer
import io.iohk.metronome.checkpointing.interpreter.messages.InterpreterMessage
import java.net.InetSocketAddress

/** Object composition, allowing overrides in integration tests. */
trait CheckpointingComposition {
  import CheckpointingCodecs._
  import DefaultInterpreterCodecs.interpreterMessageCodec

  type RemoteNetworkMessage =
    DuplexMessage[CheckpointingAgreement, CheckpointingMessage]

  type NS = RocksDBStore.Namespace

  type RNTS = NetworkTracers[Task, ECPublicKey, RemoteNetworkMessage]
  type LNTS = NetworkTracers[Task, ECPublicKey, InterpreterMessage]
  type CTS  = ConsensusTracers[Task, CheckpointingAgreement]
  type STS  = SyncTracers[Task, CheckpointingAgreement]

  /** Wire together the Checkpointing Service. */
  def compose(
      config: CheckpointingConfig
  ): Resource[Task, Unit] = {

    implicit val remoteNetworkTracers = makeRemoteNetworkTracers
    implicit val localNetworkTracers  = makeLocalNetworkTracers
    implicit val consesusTracers      = makeConsensusTracers
    implicit val syncTracers          = makeSyncTracers
    implicit val serviceTracer        = makeServiceTracer

    for {
      keyPair <- Resource.liftF(readPrivateKey(config)).map { privateKey =>
        ECKeyPair(privateKey.underlying)
      }

      remoteConnectionProvider <- makeRemoteConnectionProvider(
        config,
        keyPair
      )
      remoteConnectionManager <- makeRemoteConnectionManager(
        config,
        remoteConnectionProvider
      )
      (hotstuffNetwork, applicationNetwork) <- makeNetworks(
        remoteConnectionManager
      )

      localConnectionProvider <- makeLocalConnectionProvider(
        config,
        keyPair
      )
      localConnectionManager <- makeLocalConnectionManager(
        config,
        localConnectionProvider
      )

      db <- makeRocksDBStore(config)
      implicit0(storeRunner: KVStoreRunner[Task, NS]) = makeKVStoreRunner(db)

      blockStorage     <- makeBlockStorage(Block.genesis)
      viewStateStorage <- makeViewStateStorage(Block.genesis)
      ledgerStorage    <- makeLedgerStorage(config, Ledger.empty)

      _ <- makeBlockPruner(config, blockStorage, viewStateStorage)

      appService <- makeApplicationService(
        config,
        keyPair.pub,
        applicationNetwork,
        localConnectionManager,
        blockStorage,
        viewStateStorage,
        ledgerStorage
      )

      _ <- makeHotstuffService(
        config,
        keyPair,
        Block.genesis,
        hotstuffNetwork,
        appService,
        blockStorage,
        viewStateStorage
      )

    } yield ()
  }

  protected def makeRemoteNetworkTracers: RNTS =
    CheckpointingRemoteNetworkTracers.networkHybridLogTracers

  protected def makeLocalNetworkTracers: LNTS =
    CheckpointingLocalNetworkTracers.networkHybridLogTracers

  protected def makeConsensusTracers: CTS =
    CheckpointingConsensusTracers.consensusHybridLogTracers

  protected def makeSyncTracers: STS =
    CheckpointingSyncTracers.syncHybridLogTracers

  protected def makeServiceTracer =
    CheckpointingServiceTracers.serviceEventHybridLogTracer

  protected def makeConnectionProvider[M: Codec](
      bindAddress: InetSocketAddress,
      keyPair: ECKeyPair,
      name: String
  ) = {
    for {
      implicit0(scheduler: Scheduler) <- Resource.make(
        Task(Scheduler.io(s"scalanet-$name"))
      )(scheduler => Task(scheduler.shutdown()))

      connectionProvider <- ScalanetConnectionProvider[
        Task,
        ECPublicKey,
        M
      ](
        bindAddress = bindAddress,
        nodeKeyPair = keyPair,
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

  protected def makeRemoteConnectionProvider(
      config: CheckpointingConfig,
      keyPair: ECKeyPair
  ) = {
    makeConnectionProvider[RemoteNetworkMessage](
      bindAddress = config.remote.listen.address,
      keyPair = keyPair,
      name = "remote"
    )
  }

  protected def makeLocalConnectionProvider(
      config: CheckpointingConfig,
      keyPair: ECKeyPair
  ) = {
    makeConnectionProvider[InterpreterMessage](
      bindAddress = config.local.listen.address,
      keyPair = keyPair,
      name = "local"
    )
  }

  protected def makeRemoteConnectionManager(
      config: CheckpointingConfig,
      connectionProvider: EncryptedConnectionProvider[
        Task,
        ECPublicKey,
        RemoteNetworkMessage
      ]
  )(implicit
      networkTracers: RNTS
  ) = {
    val clusterConfig = RemoteConnectionManager.ClusterConfig(
      clusterNodes = config.federation.others.map { node =>
        node.publicKey -> node.address
      }.toSet
    )
    val retryConfig = RemoteConnectionManager.RetryConfig.default

    RemoteConnectionManager[
      Task,
      ECPublicKey,
      RemoteNetworkMessage
    ](connectionProvider, clusterConfig, retryConfig)
  }

  protected def makeLocalConnectionManager(
      config: CheckpointingConfig,
      connectionProvider: EncryptedConnectionProvider[
        Task,
        ECPublicKey,
        InterpreterMessage
      ]
  )(implicit
      networkTracers: LNTS
  ) = {
    LocalConnectionManager[
      Task,
      ECPublicKey,
      InterpreterMessage
    ](
      connectionProvider,
      targetKey = config.local.interpreter.publicKey,
      targetAddress = config.local.interpreter.address,
      retryConfig = RemoteConnectionManager.RetryConfig.default
    )
  }

  protected def readPrivateKey(
      config: CheckpointingConfig
  ): Task[ECPrivateKey] =
    config.federation.self.privateKey match {
      case Left(privateKey) =>
        privateKey.pure[Task]
      case Right(path) =>
        for {
          content <- Task(Files.readString(path))
          json = Json.fromString(content)
          privateKey <- Task.fromTry(
            CheckpointingConfigParser.ecPrivateKeyDecoder.decodeJson(json).toTry
          )
        } yield privateKey
    }

  protected def makeNetworks(
      connectionManager: RemoteConnectionManager[
        Task,
        ECPublicKey,
        RemoteNetworkMessage
      ]
  ) = {
    val network = Network
      .fromRemoteConnnectionManager[
        Task,
        CheckpointingAgreement.PKey,
        RemoteNetworkMessage
      ](
        connectionManager
      )

    for {
      (hotstuffNetwork, applicationNetwork) <- Network.splitter[
        Task,
        CheckpointingAgreement.PKey,
        RemoteNetworkMessage,
        HotStuffMessage[CheckpointingAgreement],
        CheckpointingMessage
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

  protected def makeRocksDBStore(
      config: CheckpointingConfig
  ) = {
    val dbConfig = RocksDBStore.Config.default(
      config.database.path.resolve(config.name)
    )
    for {
      dir <- Resource.liftF {
        Task {
          Files.createDirectories(dbConfig.path)
        }
      }
      blocker <- makeDBBlocker
      db      <- RocksDBStore[Task](dbConfig, CheckpointingNamespaces.all, blocker)
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

  protected def makeBlockStorage(genesis: Block)(implicit
      storeRunner: KVStoreRunner[Task, NS]
  ) = {
    implicit def `Codec[Set[T]]`[T: Codec] = {
      import scodec.codecs.implicits._
      Codec[List[T]].xmap[Set[T]](_.toSet, _.toList)
    }

    val blockStorage = new BlockStorage[NS, CheckpointingAgreement](
      blockColl =
        new KVCollection[NS, Block.Hash, Block](CheckpointingNamespaces.Block),
      blockMetaColl =
        new KVCollection[NS, Block.Hash, KVTree.NodeMeta[Block.Hash]](
          CheckpointingNamespaces.BlockMeta
        ),
      parentToChildrenColl = new KVCollection[NS, Block.Hash, Set[Block.Hash]](
        CheckpointingNamespaces.BlockToChildren
      )
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

  protected def makeViewStateStorage(genesis: Block)(implicit
      storeRunner: KVStoreRunner[Task, NS]
  ) = Resource.liftF {
    val genesisQC = QuorumCertificate[CheckpointingAgreement, Phase.Prepare](
      phase = Phase.Prepare,
      viewNumber = ViewNumber(0),
      blockHash = genesis.hash,
      signature = GroupSignature(Nil)
    )
    storeRunner.runReadWrite {
      ViewStateStorage[NS, CheckpointingAgreement](
        CheckpointingNamespaces.ViewState,
        genesis = ViewStateStorage.Bundle.fromGenesisQC(genesisQC)
      )
    }
  }

  protected def makeLedgerStorage(
      config: CheckpointingConfig,
      genesisState: Ledger
  )(implicit
      storeRunner: KVStoreRunner[Task, NS]
  ) = Resource.liftF {
    for {
      coll <- Task.pure {
        new KVCollection[NS, Ledger.Hash, Ledger](
          CheckpointingNamespaces.Ledger
        )
      }
      // Insert the genesis state straight into the underlying collection,
      // not the ringbuffer, so it doesn't get evicted if we restart the
      // app a few times.
      _ <- storeRunner.runReadWrite {
        coll.put(genesisState.hash, genesisState)
      }
      stateStorage =
        new LedgerStorage[NS](
          coll,
          ledgerMetaNamespace = CheckpointingNamespaces.LedgerMeta,
          maxHistorySize = config.database.stateHistorySize
        )
    } yield stateStorage
  }

  protected def makeBlockPruner(
      config: CheckpointingConfig,
      blockStorage: BlockStorage[NS, CheckpointingAgreement],
      viewStateStorage: ViewStateStorage[NS, CheckpointingAgreement]
  )(implicit storeRunner: KVStoreRunner[Task, NS]) =
    Concurrent[Task].background {
      storeRunner
        .runReadWrite {
          BlockPruning.prune(
            blockStorage,
            viewStateStorage,
            config.database.blockHistorySize
          )
        }
        .delayResult(config.database.pruneInterval)
        .foreverM
    }

  protected def makeApplicationService(
      config: CheckpointingConfig,
      publicKey: ECPublicKey,
      applicationNetwork: Network[
        Task,
        CheckpointingAgreement.PKey,
        CheckpointingMessage
      ],
      interpreterConnection: InterpreterConnection[Task],
      blockStorage: BlockStorage[NS, CheckpointingAgreement],
      viewStateStorage: ViewStateStorage[NS, CheckpointingAgreement],
      ledgerStorage: LedgerStorage[NS]
  )(implicit
      storeRunner: KVStoreRunner[Task, NS],
      serviceTracers: Tracer[Task, CheckpointingEvent]
  ) = {
    CheckpointingService[Task, NS](
      publicKey = publicKey,
      network = applicationNetwork,
      ledgerStorage = ledgerStorage,
      blockStorage = blockStorage,
      viewStateStorage = viewStateStorage,
      interpreterConnection = interpreterConnection,
      config = CheckpointingService.Config(
        expectCheckpointCandidateNotifications =
          config.local.expectCheckpointCandidateNotifications,
        interpreterTimeout = config.local.timeout,
        networkTimeout = config.remote.timeout
      )
    )
  }

  protected def makeHotstuffService(
      config: CheckpointingConfig,
      keyPair: ECKeyPair,
      genesis: Block,
      hotstuffNetwork: Network[
        Task,
        CheckpointingAgreement.PKey,
        HotStuffMessage[CheckpointingAgreement]
      ],
      appService: CheckpointingService[Task, NS],
      blockStorage: BlockStorage[NS, CheckpointingAgreement],
      viewStateStorage: ViewStateStorage[NS, CheckpointingAgreement]
  )(implicit
      storeRunner: KVStoreRunner[Task, NS],
      consensusTracers: CTS,
      syncTracers: STS
  ) = {
    implicit val leaderSelection = LeaderSelection.Hashing
    //implicit val signing         = new RobotSigning(genesis.hash)

    for {
      federation <- Resource.liftF {
        Task.fromEither((e: String) => new IllegalArgumentException(e)) {
          val orderedPublicKeys =
            (keyPair.pub +: config.federation.others.map(_.publicKey))
              .sortBy(_.bytes.toHex)
              .toVector

          config.federation.maxFaulty match {
            case None            => Federation(orderedPublicKeys)
            case Some(maxFaulty) => Federation(orderedPublicKeys, maxFaulty)
          }
        }
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
      protocolState = ProtocolState[CheckpointingAgreement](
        viewNumber = viewState.viewNumber.next,
        phase = Phase.Prepare,
        publicKey = keyPair.pub,
        signingKey = keyPair.prv,
        federation = federation,
        prepareQC = viewState.prepareQC,
        lockedQC = viewState.lockedQC,
        commitQC = viewState.commitQC,
        preparedBlock = preparedBlock,
        timeout = config.consensus.minTimeout,
        votes = Set.empty,
        newViews = Map.empty
      )

      _ <- HotStuffService[Task, NS, CheckpointingAgreement](
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

}

object CheckpointingComposition extends CheckpointingComposition
