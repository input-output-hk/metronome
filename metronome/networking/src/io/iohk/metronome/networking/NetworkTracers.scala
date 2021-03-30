package io.iohk.metronome.networking

import cats.implicits._
import io.iohk.metronome.tracer.Tracer

case class NetworkTracers[F[_], K, M](
    registered: Tracer[F, ConnectionHandler.HandledConnection[F, K, M]],
    deregistered: Tracer[F, ConnectionHandler.HandledConnection[F, K, M]],
    discarded: Tracer[F, ConnectionHandler.HandledConnection[F, K, M]],
    failed: Tracer[F, RemoteConnectionManager.ConnectionFailure[K]],
    error: Tracer[F, NetworkTracers.HandledConnectionError[F, K, M]],
    unknown: Tracer[F, EncryptedConnection[F, K, M]]
)

object NetworkTracers {
  import NetworkEvent._
  import ConnectionHandler.HandledConnection

  type HandledConnectionError[F[_], K, M] = (
      ConnectionHandler.HandledConnection[F, K, M],
      EncryptedConnectionProvider.ConnectionError
  )

  def apply[F[_], K, M](
      tracer: Tracer[F, NetworkEvent[K]]
  ): NetworkTracers[F, K, M] =
    NetworkTracers[F, K, M](
      registered = tracer.contramap[HandledConnection[F, K, M]] { conn =>
        ConnectionRegistered(Peer(conn.key, conn.serverAddress))
      },
      deregistered = tracer.contramap[HandledConnection[F, K, M]] { conn =>
        ConnectionDeregistered(Peer(conn.key, conn.serverAddress))
      },
      discarded = tracer.contramap[HandledConnection[F, K, M]] { conn =>
        ConnectionDiscarded(Peer(conn.key, conn.serverAddress))
      },
      failed =
        tracer.contramap[RemoteConnectionManager.ConnectionFailure[K]] { fail =>
          ConnectionFailed(
            Peer(fail.connectionRequest.key, fail.connectionRequest.address),
            fail.connectionRequest.numberOfFailures,
            fail.err
          )
        },
      error =
        tracer.contramap[HandledConnectionError[F, K, M]] { case (conn, err) =>
          ConnectionError(Peer(conn.key, conn.serverAddress), err)
        },
      unknown = tracer.contramap[EncryptedConnection[F, K, M]] { conn =>
        ConnectionUnknown((Peer.apply[K] _).tupled(conn.remotePeerInfo))
      }
    )
}
