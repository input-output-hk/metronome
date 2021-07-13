package io.iohk.metronome.networking

import cats.implicits._
import cats.effect.{Sync, Resource, Concurrent, ContextShift}
import io.iohk.metronome.networking.ConnectionHandler.MessageReceived
import monix.tail.Iterant
import monix.catnap.ConcurrentQueue

/** Network adapter for specializing messages. */
trait Network[F[_], K, M] {

  /** Receive incoming messages from the network. */
  def incomingMessages: Iterant[F, MessageReceived[K, M]]

  /** Try sending a message to a federation member, if we are connected. */
  def sendMessage(recipient: K, message: M): F[Unit]
}

object Network {

  def fromRemoteConnnectionManager[F[_]: Sync, K, M](
      manager: RemoteConnectionManager[F, K, M]
  ): Network[F, K, M] = new Network[F, K, M] {
    override def incomingMessages =
      manager.incomingMessages

    override def sendMessage(recipient: K, message: M) =
      // Not returning an error if we are trying to send to someone no longer connected,
      // this should be handled transparently, delivery is best-effort.
      manager.sendMessage(recipient, message).void
  }

  /** Consume messges from a network and dispatch them either left or right,
    * based on a splitter function. Combine messages the other way.
    */
  def splitter[F[_]: Concurrent: ContextShift, K, M, L, R](
      network: Network[F, K, M]
  )(
      split: M => Either[L, R],
      merge: Either[L, R] => M
  ): Resource[F, (Network[F, K, L], Network[F, K, R])] =
    for {
      leftQueue  <- makeQueue[F, K, L]
      rightQueue <- makeQueue[F, K, R]

      _ <- Concurrent[F].background {
        network.incomingMessages.mapEval {
          case MessageReceived(from, message) =>
            split(message) match {
              case Left(leftMessage) =>
                leftQueue.offer(MessageReceived(from, leftMessage))
              case Right(rightMessage) =>
                rightQueue.offer(MessageReceived(from, rightMessage))
            }
        }.completedL
      }

      leftNetwork = new SplitNetwork[F, K, L](
        leftQueue.poll,
        (r, m) => network.sendMessage(r, merge(Left(m)))
      )

      rightNetwork = new SplitNetwork[F, K, R](
        rightQueue.poll,
        (r, m) => network.sendMessage(r, merge(Right(m)))
      )

    } yield (leftNetwork, rightNetwork)

  private def makeQueue[F[_]: Concurrent: ContextShift, K, M] =
    Resource.liftF {
      ConcurrentQueue.unbounded[F, MessageReceived[K, M]](None)
    }

  private class SplitNetwork[F[_]: Sync, K, M](
      poll: F[MessageReceived[K, M]],
      send: (K, M) => F[Unit]
  ) extends Network[F, K, M] {
    override def incomingMessages: Iterant[F, MessageReceived[K, M]] =
      Iterant.repeatEvalF(poll)

    def sendMessage(recipient: K, message: M) =
      send(recipient, message)
  }
}
