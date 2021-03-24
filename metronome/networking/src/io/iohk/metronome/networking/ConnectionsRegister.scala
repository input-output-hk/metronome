package io.iohk.metronome.networking

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import io.iohk.metronome.networking.ConnectionHandler.HandledConnection
import cats.implicits._

class ConnectionsRegister[F[_]: Concurrent, K, M](
    register: Ref[F, Map[K, HandledConnection[F, K, M]]]
) {

  def registerIfAbsent(
      connection: HandledConnection[F, K, M]
  ): F[Option[HandledConnection[F, K, M]]] = {
    register.modify { current =>
      val connectionKey = connection.key

      if (current.contains(connectionKey)) {
        (current, current.get(connectionKey))
      } else {
        (current.updated(connectionKey, connection), None)
      }
    }
  }

  def isNewConnection(connectionKey: K): F[Boolean] = {
    register.get.map(currentState => !currentState.contains(connectionKey))
  }

  def deregisterConnection(
      connection: HandledConnection[F, K, M]
  ): F[Unit] = {
    register.update(current => current - (connection.key))
  }

  def getAllRegisteredConnections: F[Set[HandledConnection[F, K, M]]] = {
    register.get.map(m => m.values.toSet)
  }

  def getConnection(
      connectionKey: K
  ): F[Option[HandledConnection[F, K, M]]] =
    register.get.map(connections => connections.get(connectionKey))

}

object ConnectionsRegister {
  def empty[F[_]: Concurrent, K, M]: F[ConnectionsRegister[F, K, M]] = {
    Ref
      .of(Map.empty[K, HandledConnection[F, K, M]])
      .map(ref => new ConnectionsRegister[F, K, M](ref))
  }
}
