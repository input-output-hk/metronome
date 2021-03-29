package io.iohk.metronome.networking

import java.net.InetSocketAddress

/** Events we want to trace. */
sealed trait NetworkEvent[K]

object NetworkEvent {
  case class ConnectionRegistered[K](
      key: K,
      serverAddress: InetSocketAddress
  ) extends NetworkEvent[K]

  case class ConnectionDeregistered[K](
      key: K,
      serverAddress: InetSocketAddress
  ) extends NetworkEvent[K]

  case class ConnectionDiscarded[K](
      key: K,
      serverAddress: InetSocketAddress
  ) extends NetworkEvent[K]

  case class ConnectionFailed[K](
      failure: RemoteConnectionManager.ConnectionFailure[K]
  ) extends NetworkEvent[K]

}
