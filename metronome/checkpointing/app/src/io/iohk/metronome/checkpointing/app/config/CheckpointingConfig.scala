package io.iohk.metronome.checkpointing.app.config

import io.iohk.metronome.crypto.{ECPublicKey, ECPrivateKey}
import java.nio.file.Path
import scala.concurrent.duration.FiniteDuration

case class CheckpointingConfig(
    federation: CheckpointingConfig.Federation,
    network: CheckpointingConfig.Network,
    db: CheckpointingConfig.Database
)

object CheckpointingConfig {
  case class Federation(
      others: List[Node],
      self: Node,
      privateKey: ECPrivateKey
  )
  case class Node(
      host: String,
      port: Int,
      publicKey: ECPublicKey
  )
  case class Network(
      bindHost: String,
      bindPort: Int,
      timeout: FiniteDuration
  )
  case class Database(
      path: Path,
      stateHistorySize: Int,
      blockHistorySize: Int,
      pruneInterval: FiniteDuration
  )
}
