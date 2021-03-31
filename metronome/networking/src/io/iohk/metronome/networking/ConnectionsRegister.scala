package io.iohk.metronome.networking

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import io.iohk.metronome.networking.ConnectionHandler.HandledConnection
import cats.implicits._

class ConnectionsRegister[F[_]: Concurrent, K, M](
    registerRef: Ref[F, Map[K, HandledConnection[F, K, M]]]
) {

  def registerIfAbsent(
      connection: HandledConnection[F, K, M]
  ): F[Option[HandledConnection[F, K, M]]] = {
    registerRef.modify { register =>
      val connectionKey = connection.key

      if (register.contains(connectionKey)) {
        (register, register.get(connectionKey))
      } else {
        (register.updated(connectionKey, connection), None)
      }
    }
  }

  def registerConnection(connection: HandledConnection[F, K, M]): F[Unit] = {
    registerRef.update(register => register + (connection.key -> connection))
  }

  def isNewConnection(connectionKey: K): F[Boolean] = {
    registerRef.get.map(register => !register.contains(connectionKey))
  }

  def deregisterConnection(
      connection: HandledConnection[F, K, M]
  ): F[Unit] = {
    registerRef.update(register => register - (connection.key))
  }

  def getAllRegisteredConnections: F[Set[HandledConnection[F, K, M]]] = {
    registerRef.get.map(register => register.values.toSet)
  }

  def getConnection(
      connectionKey: K
  ): F[Option[HandledConnection[F, K, M]]] =
    registerRef.get.map(register => register.get(connectionKey))

}

object ConnectionsRegister {
  def empty[F[_]: Concurrent, K, M]: F[ConnectionsRegister[F, K, M]] = {
    Ref
      .of(Map.empty[K, HandledConnection[F, K, M]])
      .map(ref => new ConnectionsRegister[F, K, M](ref))
  }
}
