package io.iohk.metronome.checkpointing.app.config

import io.iohk.metronome.crypto.{ECPublicKey, ECPrivateKey}
import java.nio.file.Path
import java.net.InetSocketAddress
import scala.concurrent.duration.FiniteDuration

case class CheckpointingConfig(
    federation: CheckpointingConfig.Federation,
    consensus: CheckpointingConfig.Consensus,
    remote: CheckpointingConfig.RemoteNetwork,
    local: CheckpointingConfig.LocalNetwork,
    database: CheckpointingConfig.Database
)

object CheckpointingConfig {
  trait HasAddress {
    def host: String
    def port: Int
    lazy val address = new InetSocketAddress(host, port)
  }

  case class Federation(
      self: Node,
      privateKey: ECPrivateKey,
      others: List[Node],
      maxFaulty: Option[Int]
  )

  case class Consensus(
      minTimeout: FiniteDuration,
      maxTimeout: FiniteDuration,
      timeoutFactor: Double
  )

  case class Node(
      val host: String,
      val port: Int,
      publicKey: ECPublicKey
  ) extends HasAddress

  case class Socket(
      val host: String,
      val port: Int
  ) extends HasAddress

  case class RemoteNetwork(
      listen: Socket,
      timeout: FiniteDuration
  )

  case class LocalNetwork(
      listen: Socket,
      target: Socket,
      timeout: FiniteDuration
  )

  case class Database(
      path: Path,
      stateHistorySize: Int,
      blockHistorySize: Int,
      pruneInterval: FiniteDuration
  )
}
