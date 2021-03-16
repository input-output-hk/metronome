package io.iohk.metronome.hotstuff.service

import io.iohk.metronome.hotstuff.service.EncryptedConnectionProvider.{ChannelError, HandshakeFailed}

trait EncryptedConnection[F[_], K, M] {
  def info: K
  def sendMessage(m: M): F[Unit]
  def incomingMessage: F[Option[Either[ChannelError, M]]]
  def close(): F[Unit]
}


trait EncryptedConnectionProvider[F[_], K, M] {
  def connectTo(k: K): F[EncryptedConnection[F, K, M]]
  def incomingConnection: F[Option[Either[HandshakeFailed, EncryptedConnection[F, K, M]]]]
}


object EncryptedConnectionProvider {
  case class HandshakeFailed(ex: Throwable)

  sealed trait ChannelError
  case object DecodingError extends ChannelError
  case class UnexpectedError(ex: Throwable) extends ChannelError

}
