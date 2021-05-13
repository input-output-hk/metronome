package io.iohk.metronome.examples.robot.app

import cats.effect.{ExitCode, Resource}
import monix.eval.{Task, TaskApp}
import io.iohk.metronome.crypto.ECKeyPair
import io.iohk.metronome.hotstuff.consensus.{Federation, LeaderSelection}
import io.iohk.metronome.hotstuff.service.messages.DuplexMessage
import io.iohk.metronome.networking.{
  ScalanetConnectionProvider,
  RemoteConnectionManager
}
import io.iohk.metronome.examples.robot.RobotAgreement
import io.iohk.metronome.examples.robot.codecs.RobotCodecs
import io.iohk.metronome.examples.robot.service.messages.RobotMessage
import io.iohk.metronome.examples.robot.app.config.{
  RobotConfigParser,
  RobotConfig
}
import io.iohk.metronome.examples.robot.app.tracing.RobotNetworkTracers
import io.iohk.scalanet.peergroup.dynamictls.DynamicTLSPeerGroup
import java.security.SecureRandom
import scopt.OParser

object RobotApp extends TaskApp {
  type NetworkMessage = DuplexMessage[RobotAgreement, RobotMessage]

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
            0 <= i && i < config.nodes.length,
            (),
            s"Must be between 0 and ${config.nodes.length - 1}"
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

  def compose(
      opts: CommandLineOptions,
      config: RobotConfig
  ): Resource[Task, Unit] = {
    import RobotCodecs.duplexMessageCodec
    import RobotNetworkTracers.networkTracers
    implicit val scheduler       = this.scheduler
    implicit val leaderSelection = LeaderSelection.Hashing

    val federation = Federation(config.nodes.map(_.publicKey).toVector)
    val localNode  = config.nodes(opts.nodeIndex)

    val retryConfig = RemoteConnectionManager.RetryConfig.default
    val clusterConfig = RemoteConnectionManager.ClusterConfig(
      clusterNodes = config.nodes.map { node =>
        node.publicKey -> node.address
      }.toSet
    )

    for {
      connectionProvider <- ScalanetConnectionProvider[
        Task,
        RobotAgreement.PKey,
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
        RobotAgreement.PKey,
        NetworkMessage
      ](connectionProvider, clusterConfig, retryConfig)
    } yield ()
  }

}
