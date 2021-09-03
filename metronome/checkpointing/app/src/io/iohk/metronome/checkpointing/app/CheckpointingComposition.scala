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
import io.iohk.metronome.checkpointing.service.messages.CheckpointingMessage
import io.iohk.metronome.checkpointing.models.{Block, Ledger}
import io.iohk.metronome.hotstuff.consensus.{ViewNumber}
import io.iohk.metronome.hotstuff.consensus.basic.{QuorumCertificate, Phase}
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
  NetworkTracers,
  Network
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
import io.circe.Json
import java.security.SecureRandom
import java.nio.file.Files
import monix.eval.Task
import monix.execution.Scheduler
import scodec.Codec

/** Object composition, allowing overrides in integration tests. */
trait CheckpointingComposition {
  import CheckpointingCodecs._

  type NetworkMessage =
    DuplexMessage[CheckpointingAgreement, CheckpointingMessage]

  type ConnectionManager =
    RemoteConnectionManager[Task, ECPublicKey, NetworkMessage]

  type NS = RocksDBStore.Namespace

  type NTS = NetworkTracers[Task, ECPublicKey, NetworkMessage]
  type CTS = ConsensusTracers[Task, CheckpointingAgreement]
  type STS = SyncTracers[Task, CheckpointingAgreement]

  /** Wire together the Checkpointing Service. */
  def compose(
      config: CheckpointingConfig
  ): Resource[Task, Unit] = {

    implicit val networkTracers: NTS = makeNetworkTracers
    // implicit val consesusTracers: CTS = makeConsensusTracers
    // implicit val syncTracers: STS     = makeSyncTracers
    // implicit val serviceTracer        = makeServiceTracer

    for {
      connectionManager <- makeConnectionManager(config)

      (hotstuffNetwork, applicationNetwork) <- makeNetworks(connectionManager)

      db <- makeRocksDBStore(config)
      implicit0(storeRunner: KVStoreRunner[Task, NS]) = makeKVStoreRunner(db)

      blockStorage     <- makeBlockStorage(Block.genesis)
      viewStateStorage <- makeViewStateStorage(Block.genesis)
      stateStorage     <- makeStateStorage(config, Ledger.empty)

      _ <- makeBlockPruner(config, blockStorage, viewStateStorage)

    } yield ()
  }

  protected def makeNetworkTracers =
    CheckpointingNetworkTracers.networkHybridLogTracers

  protected def makeConsensusTracers =
    CheckpointingConsensusTracers.consensusHybridLogTracers

  protected def makeSyncTracers =
    CheckpointingSyncTracers.syncHybridLogTracers

  protected def makeServiceTracer =
    CheckpointingServiceTracers.serviceEventHybridLogTracer

  protected def makeConnectionManager(
      config: CheckpointingConfig
  )(implicit
      networkTracers: NTS
  ): Resource[Task, ConnectionManager] =
    for {
      connectionProvider <- makeConnectionProvider(config)
      connectionManager  <- makeConnectionManager(config, connectionProvider)
    } yield connectionManager

  protected def makeConnectionManager(
      config: CheckpointingConfig,
      connectionProvider: EncryptedConnectionProvider[
        Task,
        ECPublicKey,
        NetworkMessage
      ]
  )(implicit
      networkTracers: NTS
  ): Resource[Task, ConnectionManager] = {

    val clusterConfig = RemoteConnectionManager.ClusterConfig(
      clusterNodes = config.federation.others.map { node =>
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

  protected def makeConnectionProvider(
      config: CheckpointingConfig
  ) = {
    for {
      implicit0(scheduler: Scheduler) <- Resource.make(
        Task(Scheduler.io("scalanet"))
      )(scheduler => Task(scheduler.shutdown()))

      privateKey <- Resource.liftF(readPrivateKey(config))

      connectionProvider <- ScalanetConnectionProvider[
        Task,
        ECPublicKey,
        NetworkMessage
      ](
        bindAddress = config.remote.listen.address,
        nodeKeyPair = ECKeyPair(privateKey.underlying),
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
        NetworkMessage
      ]
  ) = {
    val network = Network
      .fromRemoteConnnectionManager[
        Task,
        CheckpointingAgreement.PKey,
        NetworkMessage
      ](
        connectionManager
      )

    for {
      (hotstuffNetwork, applicationNetwork) <- Network.splitter[
        Task,
        CheckpointingAgreement.PKey,
        NetworkMessage,
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

  protected def makeStateStorage(
      config: CheckpointingConfig,
      genesisState: Ledger
  )(implicit
      storeRunner: KVStoreRunner[Task, NS]
  ) = Resource.liftF {
    for {
      coll <- Task.pure {
        new KVCollection[NS, Ledger.Hash, Ledger](CheckpointingNamespaces.State)
      }
      // Insert the genesis state straight into the underlying collection,
      // not the ringbuffer, so it doesn't get evicted if we restart the
      // app a few times.
      _ <- storeRunner.runReadWrite {
        coll.put(genesisState.hash, genesisState)
      }
      stateStorage =
        new KVRingBuffer[NS, Ledger.Hash, Ledger](
          coll,
          metaNamespace = CheckpointingNamespaces.StateMeta,
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

}

object CheckpointingComposition extends CheckpointingComposition
