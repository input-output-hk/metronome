package io.iohk.metronome.hotstuff.service

import cats.effect.{Resource, Sync}
import io.iohk.metronome.hotstuff.service.EncryptedConnectionProvider.{
  ChannelError,
  DecodingError,
  HandshakeFailed,
  UnexpectedError
}
import io.iohk.scalanet.peergroup.Channel
import io.iohk.scalanet.peergroup.PeerGroup.ServerEvent
import io.iohk.scalanet.peergroup.dynamictls.DynamicTLSPeerGroup.{
  Config,
  FramingConfig,
  PeerInfo
}
import io.iohk.scalanet.peergroup.dynamictls.{DynamicTLSPeerGroup, Secp256k1}
import monix.eval.{Task, TaskLift}
import monix.execution.Scheduler
import org.bouncycastle.crypto.AsymmetricCipherKeyPair
import scodec.Codec

import java.net.InetSocketAddress
import java.security.SecureRandom

class ScalanetConnectionProvider {
  trait PeerInfoTransform[A] {
    def toPeerInfo(a: A): DynamicTLSPeerGroup.PeerInfo
    def fromPeerInfo(info: DynamicTLSPeerGroup.PeerInfo): A
  }

  object PeerInfoTransform {
    def apply[T](implicit tr: PeerInfoTransform[T]): PeerInfoTransform[T] = tr
    object ops {
      implicit class PeerInfoTransformOps[A: PeerInfoTransform](a: A) {
        def toPeerInfo: DynamicTLSPeerGroup.PeerInfo =
          PeerInfoTransform[A].toPeerInfo(a)
      }
      implicit class PeerInfoTransformOps1[A: PeerInfoTransform](a: PeerInfo) {
        def fromPeerInfo: A =
          PeerInfoTransform[A].fromPeerInfo(a)
      }
    }
  }

  import PeerInfoTransform.ops._

  private class ScalanetEncryptedConnection[F[_]: TaskLift, K: PeerInfoTransform, M: Codec](
      underlyingChannel: Channel[PeerInfo, M],
      underlyingChannelRelease: F[Unit]
  ) extends EncryptedConnection[F, K, M] {
    override def close(): F[Unit] = underlyingChannelRelease

    override def info: K = underlyingChannel.to.fromPeerInfo

    override def sendMessage(m: M): F[Unit] =
      TaskLift[F].apply(underlyingChannel.sendMessage(m))

    override def incomingMessage: F[Option[Either[ChannelError, M]]] = {
      TaskLift[F].apply(underlyingChannel.nextChannelEvent.map {
        case Some(event) =>
          event match {
            case Channel.MessageReceived(m) => Some(Right(m))
            case Channel.UnexpectedError(e) => Some(Left(UnexpectedError(e)))
            case Channel.DecodingError      => Some(Left(DecodingError))
          }
        case None => None
      })
    }
  }

  def scalanetProvider[F[_]: Sync: TaskLift, K: PeerInfoTransform, M: Codec](
      bindAddress: InetSocketAddress,
      nodeKeyPair: AsymmetricCipherKeyPair,
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
              nodeKeyPair,
              secureRandom,
              useNativeTlsImplementation,
              framingConfig,
              maxIncomingQueueSizePerPeer,
              None
            )
        )
      )
      pg <- DynamicTLSPeerGroup[M](config).mapK(TaskLift.apply)
    } yield new EncryptedConnectionProvider[F, K, M] {
      override def connectTo(k: K): F[EncryptedConnection[F, K, M]] = {
        TaskLift[F].apply(pg.client(k.toPeerInfo).allocated.attempt.flatMap {
          case Left(value) => Task.raiseError(value)
          case Right((channel, release)) =>
            Task.now(
              new ScalanetEncryptedConnection(
                channel,
                TaskLift[F].apply(release)
              )
            )
        })
      }

      override def incomingConnection
          : F[Option[Either[HandshakeFailed, EncryptedConnection[F, K, M]]]] = {
        TaskLift[F].apply(pg.nextServerEvent.map {
          case Some(ev) =>
            ev match {
              case ServerEvent.ChannelCreated(channel, release) =>
                Some(
                  Right(
                    new ScalanetEncryptedConnection(
                      channel,
                      TaskLift[F].apply(release)
                    )
                  )
                )
              case ServerEvent.HandshakeFailed(failure) =>
                Some(Left(HandshakeFailed(failure)))
            }
          case None => None
        })
      }
    }
  }
}
