package io.iohk.metronome.networking

import java.net.InetSocketAddress

/** Events we want to trace. */
sealed trait NetworkEvent[K, +M]

object NetworkEvent {
  import ConnectionHandler.HandledConnection.HandledConnectionDirection

  case class Peer[K](key: K, address: InetSocketAddress)

  /** The connection to/from the peer has been added to the register. */
  case class ConnectionRegistered[K](
      peer: Peer[K],
      direction: HandledConnectionDirection
  ) extends NetworkEvent[K, Nothing]

  /** The connection to/from the peer has been closed and removed from the register. */
  case class ConnectionDeregistered[K](
      peer: Peer[K],
      direction: HandledConnectionDirection
  ) extends NetworkEvent[K, Nothing]

  /** We had two connections to/from the peer and discarded one of them. */
  case class ConnectionDiscarded[K](
      peer: Peer[K],
      direction: HandledConnectionDirection
  ) extends NetworkEvent[K, Nothing]

  /** Failed to establish connection to remote peer. */
  case class ConnectionFailed[K](
      peer: Peer[K],
      numberOfFailures: Int,
      error: Throwable
  ) extends NetworkEvent[K, Nothing]

  /** Error reading data from a connection. */
  case class ConnectionReceiveError[K](
      peer: Peer[K],
      error: EncryptedConnectionProvider.ConnectionError
  ) extends NetworkEvent[K, Nothing]

  /** Error sending data over a connection, already disconnected. */
  case class ConnectionSendError[K](
      peer: Peer[K]
  ) extends NetworkEvent[K, Nothing]

  /** Incoming connection from someone outside the federation. */
  case class ConnectionUnknown[K](peer: Peer[K])
      extends NetworkEvent[K, Nothing]

  /** Received incoming message from peer. */
  case class MessageReceived[K, M](peer: Peer[K], message: M)
      extends NetworkEvent[K, M]

  /** Sent outgoing message to peer. */
  case class MessageSent[K, M](peer: Peer[K], message: M)
      extends NetworkEvent[K, M]

}
