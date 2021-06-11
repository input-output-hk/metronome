package io.iohk.metronome.networking

import cats.implicits._
import cats.effect.{Concurrent, Timer, Resource, ContextShift}
import java.net.InetSocketAddress
import monix.eval.{TaskLift, TaskLike}
import monix.tail.Iterant
import scodec.Codec

trait LocalConnectionManager[F[_], K, M] {
  def isConnected: F[Boolean]
  def incomingMessages: Iterant[F, M]
  def sendMessage(
      message: M
  ): F[Either[ConnectionHandler.ConnectionAlreadyClosedException[K], Unit]]
}

/** Connect to a single local process and keep the connection alive. */
object LocalConnectionManager {

  def apply[
      F[_]: Concurrent: TaskLift: TaskLike: Timer: ContextShift,
      K: Codec,
      M: Codec
  ](
      encryptedConnectionsProvider: EncryptedConnectionProvider[F, K, M],
      targetKey: K,
      targetAddress: InetSocketAddress,
      retryConfig: RemoteConnectionManager.RetryConfig
  )(implicit
      tracers: NetworkTracers[F, K, M]
  ): Resource[F, LocalConnectionManager[F, K, M]] = {
    for {
      remoteConnectionManager <- RemoteConnectionManager[F, K, M](
        encryptedConnectionsProvider,
        RemoteConnectionManager.ClusterConfig[K](
          Set(targetKey -> targetAddress)
        ),
        retryConfig
      )
      localConnectionManager = new LocalConnectionManager[F, K, M] {
        override def isConnected =
          remoteConnectionManager.getAcquiredConnections.map(
            _.contains(targetKey)
          )

        override def incomingMessages =
          remoteConnectionManager.incomingMessages.map {
            case ConnectionHandler.MessageReceived(_, m) => m
          }

        override def sendMessage(message: M) =
          remoteConnectionManager.sendMessage(targetKey, message)
      }
    } yield localConnectionManager
  }
}
