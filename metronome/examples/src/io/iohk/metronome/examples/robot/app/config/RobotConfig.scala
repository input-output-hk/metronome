package io.iohk.metronome.examples.robot.app.config

import io.iohk.metronome.crypto.{ECPublicKey, ECPrivateKey}
import java.net.InetSocketAddress
import scala.concurrent.duration.FiniteDuration
import java.nio.file.Path

case class RobotConfig(
    network: RobotConfig.Network,
    model: RobotConfig.Model,
    db: RobotConfig.Database
)
object RobotConfig {
  case class Network(
      nodes: List[RobotConfig.Node],
      timeout: FiniteDuration
  )

  case class Node(
      host: String,
      port: Int,
      publicKey: ECPublicKey,
      // Because this is just an example application, we also have the private key
      // for each node, so we can just strat one of them by index.
      privateKey: ECPrivateKey
  ) {
    lazy val address = new InetSocketAddress(host, port)
  }

  case class Model(
      maxRow: Int,
      maxCol: Int,
      simulatedDecisionTime: FiniteDuration
  )

  case class Database(
      path: Path
  )
}
