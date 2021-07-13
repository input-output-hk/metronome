package io.iohk.metronome.networking

import io.iohk.metronome.networking.EncryptedConnectionProvider.{
  ConnectionError,
  HandshakeFailed
}

import java.net.InetSocketAddress

trait EncryptedConnection[F[_], K, M] {
  def localAddress: InetSocketAddress
  def remotePeerInfo: (K, InetSocketAddress)
  def sendMessage(m: M): F[Unit]
  def incomingMessage: F[Option[Either[ConnectionError, M]]]
  def close: F[Unit]
}

trait EncryptedConnectionProvider[F[_], K, M] {
  def localPeerInfo: (K, InetSocketAddress)
  def connectTo(
      k: K,
      address: InetSocketAddress
  ): F[EncryptedConnection[F, K, M]]
  def incomingConnection
      : F[Option[Either[HandshakeFailed, EncryptedConnection[F, K, M]]]]
}

object EncryptedConnectionProvider {
  case class HandshakeFailed(ex: Throwable, remoteAddress: InetSocketAddress)

  sealed trait ConnectionError
  case object DecodingError                 extends ConnectionError
  case class UnexpectedError(ex: Throwable) extends ConnectionError

  case class ConnectionAlreadyClosed(address: InetSocketAddress)
      extends RuntimeException

}
