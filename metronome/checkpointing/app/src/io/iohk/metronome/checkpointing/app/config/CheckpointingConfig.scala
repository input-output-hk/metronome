package io.iohk.metronome.checkpointing.app.config

import io.iohk.metronome.crypto.{ECPublicKey, ECPrivateKey}
import java.nio.file.Path
import scala.concurrent.duration.FiniteDuration

case class CheckpointingConfig(
    federation: CheckpointingConfig.Federation,
    network: CheckpointingConfig.Network,
    db: CheckpointingConfig.Database,
    consensus: CheckpointingConfig.Consensus
)

object CheckpointingConfig {
  case class Federation(
      self: Node,
      privateKey: ECPrivateKey,
      others: List[Node],
      maxFaulty: Option[Int]
  )
  case class Node(
      host: String,
      port: Int,
      publicKey: ECPublicKey
  )
  case class Network(
      host: String,
      port: Int,
      timeout: FiniteDuration
  )
  case class Database(
      path: Path,
      stateHistorySize: Int,
      blockHistorySize: Int,
      pruneInterval: FiniteDuration
  )
  case class Consensus(
      minTimeout: FiniteDuration,
      maxTimeout: FiniteDuration,
      timeoutFactor: Double
  )
}
