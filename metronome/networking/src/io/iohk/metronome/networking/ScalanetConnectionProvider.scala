package io.iohk.metronome.networking

import cats.effect.{Resource, Sync}
import io.iohk.metronome.crypto.ECKeyPair
import io.iohk.metronome.networking.EncryptedConnectionProvider.{
  ConnectionAlreadyClosed,
  ConnectionError,
  DecodingError,
  HandshakeFailed,
  UnexpectedError
}
import io.iohk.scalanet.peergroup.PeerGroup.{
  ChannelBrokenException,
  ServerEvent
}
import io.iohk.scalanet.peergroup.dynamictls.DynamicTLSPeerGroup.{
  Config,
  FramingConfig,
  PeerInfo
}
import io.iohk.scalanet.peergroup.dynamictls.{DynamicTLSPeerGroup, Secp256k1}
import io.iohk.scalanet.peergroup.{Channel, InetMultiAddress}
import monix.eval.{Task, TaskLift}
import monix.execution.Scheduler
import scodec.Codec

import java.net.InetSocketAddress
import java.security.SecureRandom

object ScalanetConnectionProvider {
  private class ScalanetEncryptedConnection[F[_]: TaskLift, K: Codec, M: Codec](
      underlyingChannel: Channel[PeerInfo, M],
      underlyingChannelRelease: F[Unit],
      channelKey: K
  ) extends EncryptedConnection[F, K, M] {

    override def close: F[Unit] = underlyingChannelRelease

    override val localAddress: InetSocketAddress = (
      underlyingChannel.from.address.inetSocketAddress
    )

    override val remotePeerInfo: (K, InetSocketAddress) = (
      channelKey,
      underlyingChannel.to.address.inetSocketAddress
    )

    override def sendMessage(m: M): F[Unit] = {
      TaskLift[F].apply(underlyingChannel.sendMessage(m).onErrorRecoverWith {
        case _: ChannelBrokenException[_] =>
          Task.raiseError(
            ConnectionAlreadyClosed(
              underlyingChannel.to.address.inetSocketAddress
            )
          )
      })
    }

    override def incomingMessage: F[Option[Either[ConnectionError, M]]] =
      TaskLift[F].apply(nextNonIdleMessage)

    private val nextNonIdleMessage: Task[Option[Either[ConnectionError, M]]] = {
      underlyingChannel.nextChannelEvent.flatMap {
        case Some(event) =>
          event match {
            case Channel.MessageReceived(m) =>
              Task.pure(Some(Right(m)))
            case Channel.UnexpectedError(e) =>
              Task.pure(Some(Left(UnexpectedError(e))))
            case Channel.DecodingError =>
              Task.pure(Some(Left(DecodingError)))
            case Channel.ChannelIdle(_, _) =>
              nextNonIdleMessage
          }
        case None =>
          Task.pure(None)
      }
    }
  }

  private object ScalanetEncryptedConnection {
    def apply[F[_]: TaskLift, K: Codec, M: Codec](
        channel: Channel[PeerInfo, M],
        channelRelease: Task[Unit]
    ): Task[EncryptedConnection[F, K, M]] = {

      Task
        .fromTry(Codec[K].decodeValue(channel.to.id).toTry)
        .map { key =>
          new ScalanetEncryptedConnection[F, K, M](
            channel,
            TaskLift[F].apply(channelRelease),
            key
          )
        }
        .onErrorHandleWith { e =>
          channelRelease.flatMap(_ => Task.raiseError(e))
        }

    }

  }

  // Codec constraint for K is necessary as scalanet require peer key to be in BitVector format
  def scalanetProvider[F[_]: Sync: TaskLift, K: Codec, M: Codec](
      bindAddress: InetSocketAddress,
      nodeKeyPair: ECKeyPair,
      secureRandom: SecureRandom,
      useNativeTlsImplementation: Boolean,
      framingConfig: FramingConfig,
      maxIncomingQueueSizePerPeer: Int
  )(implicit
      sch: Scheduler
  ): Resource[F, EncryptedConnectionProvider[F, K, M]] = {
    for {
      config <- Resource.liftF[F, Config](
        Sync[F].fromTry(
          DynamicTLSPeerGroup
            .Config(
              bindAddress,
              Secp256k1,
              nodeKeyPair.underlying,
              secureRandom,
              useNativeTlsImplementation,
              framingConfig,
              maxIncomingQueueSizePerPeer,
              incomingConnectionsThrottling = None,
              stalePeerDetectionConfig = None
            )
        )
      )
      pg <- DynamicTLSPeerGroup[M](config).mapK(TaskLift.apply)
      local <- Resource.pure(
        (
          Codec[K].decodeValue(pg.processAddress.id).require,
          pg.processAddress.address.inetSocketAddress
        )
      )

    } yield new EncryptedConnectionProvider[F, K, M] {
      override def localPeerInfo: (K, InetSocketAddress) = local

      import cats.implicits._

      /** Connects to remote node, creating new connection with each call
        *
        * @param k, key of the remote node
        * @param address, address of the remote node
        */
      override def connectTo(
          k: K,
          address: InetSocketAddress
      ): F[EncryptedConnection[F, K, M]] = {
        val encodedKey = Codec[K].encode(k).require
        pg.client(PeerInfo(encodedKey, InetMultiAddress(address)))
          .mapK[Task, F](TaskLift[F])
          .allocated
          .map { case (channel, release) =>
            new ScalanetEncryptedConnection(channel, release, k)
          }
      }

      override def incomingConnection
          : F[Option[Either[HandshakeFailed, EncryptedConnection[F, K, M]]]] = {
        TaskLift[F].apply(pg.nextServerEvent.flatMap {
          case Some(ev) =>
            ev match {
              case ServerEvent.ChannelCreated(channel, release) =>
                ScalanetEncryptedConnection[F, K, M](channel, release).map {
                  connection =>
                    Some(Right(connection))
                }

              case ServerEvent.HandshakeFailed(failure) =>
                Task.now(
                  Some(
                    Left(
                      HandshakeFailed(
                        failure,
                        failure.to.address.inetSocketAddress
                      )
                    )
                  )
                )

            }
          case None => Task.now(None)
        })
      }
    }
  }
}
