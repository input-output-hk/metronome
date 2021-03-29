package io.iohk.metronome.networking

import cats.implicits._
import io.iohk.metronome.tracer.Tracer

case class NetworkTracers[F[_], K, M](
    registered: Tracer[F, ConnectionHandler.HandledConnection[F, K, M]],
    deregistered: Tracer[F, ConnectionHandler.HandledConnection[F, K, M]],
    discarded: Tracer[F, ConnectionHandler.HandledConnection[F, K, M]],
    failed: Tracer[F, RemoteConnectionManager.ConnectionFailure[K]]
)

object NetworkTracers {
  import NetworkEvent._
  import ConnectionHandler.HandledConnection

  def apply[F[_], K, M](
      tracer: Tracer[F, NetworkEvent[K]]
  ): NetworkTracers[F, K, M] =
    NetworkTracers[F, K, M](
      registered = tracer.contramap[HandledConnection[F, K, M]] { conn =>
        ConnectionRegistered(conn.key, conn.serverAddress)
      },
      deregistered = tracer.contramap[HandledConnection[F, K, M]] { conn =>
        ConnectionDeregistered(conn.key, conn.serverAddress)
      },
      discarded = tracer.contramap[HandledConnection[F, K, M]] { conn =>
        ConnectionDiscarded(conn.key, conn.serverAddress)
      },
      failed = tracer.contramap[RemoteConnectionManager.ConnectionFailure[K]] {
        ConnectionFailed(_)
      }
    )
}
