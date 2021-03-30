package io.iohk.metronome.networking

import java.net.InetSocketAddress

/** Events we want to trace. */
sealed trait NetworkEvent[K]

object NetworkEvent {

  case class Peer[K](key: K, address: InetSocketAddress)

  /** The connection to/from the peer has been added to the register. */
  case class ConnectionRegistered[K](peer: Peer[K]) extends NetworkEvent[K]

  /** The connection to/from the peer has been closed and removed from the register. */
  case class ConnectionDeregistered[K](peer: Peer[K]) extends NetworkEvent[K]

  /** We had two connections to/from the peer and discarded one of them. */
  case class ConnectionDiscarded[K](peer: Peer[K]) extends NetworkEvent[K]

  /** Failed to establish connection to remote peer. */
  case class ConnectionFailed[K](
      peer: Peer[K],
      numberOfFailures: Int,
      error: Throwable
  ) extends NetworkEvent[K]

  /** An established connection has experienced an error. */
  case class ConnectionError[K](
      peer: Peer[K],
      error: EncryptedConnectionProvider.ConnectionError
  ) extends NetworkEvent[K]

}
