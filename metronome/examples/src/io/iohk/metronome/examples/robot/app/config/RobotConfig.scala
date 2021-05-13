package io.iohk.metronome.examples.robot.app.config

import io.iohk.metronome.crypto.{ECPublicKey, ECPrivateKey}
import java.net.InetSocketAddress

case class RobotConfig(
    nodes: List[RobotConfig.Node]
)
object RobotConfig {
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
}
