package io.iohk.metronome.hotstuff.service

import cats.implicits._
import cats.effect.{Sync, Resource, Concurrent, ContextShift}
import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import io.iohk.metronome.networking.ConnectionHandler.MessageReceived
import io.iohk.metronome.networking.RemoteConnectionManager
import monix.tail.Iterant
import monix.catnap.ConcurrentQueue

/** Network adapter for specialising messages. */
trait Network[F[_], A <: Agreement, M] {

  /** Receive incoming messages from the network. */
  def incomingMessages: Iterant[F, MessageReceived[A#PKey, M]]

  /** Try sending a message to a federation member, if we are connected. */
  def sendMessage(recipient: A#PKey, message: M): F[Unit]
}

object Network {

  def fromRemoteConnnectionManager[F[_]: Sync, A <: Agreement, M](
      manager: RemoteConnectionManager[F, A#PKey, M]
  ): Network[F, A, M] = new Network[F, A, M] {
    override def incomingMessages =
      manager.incomingMessages

    override def sendMessage(recipient: A#PKey, message: M) =
      // Not returning an error if we are trying to send to someone no longer connected,
      // this should be handled transparently, delivery is best-effort.
      manager.sendMessage(recipient, message).void
  }

  /** Consume messges from a network and dispatch them either left or right,
    * based on a splitter function. Combine messages the other way.
    */
  def splitter[F[_]: Concurrent: ContextShift, A <: Agreement, M, L, R](
      network: Network[F, A, M]
  )(
      split: M => Either[L, R],
      merge: Either[L, R] => M
  ): Resource[F, (Network[F, A, L], Network[F, A, R])] =
    for {
      leftQueue  <- makeQueue[F, A, L]
      rightQueue <- makeQueue[F, A, R]

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

      leftNetwork = new SplitNetwork[F, A, L](
        leftQueue.poll,
        (r, m) => network.sendMessage(r, merge(Left(m)))
      )

      rightNetwork = new SplitNetwork[F, A, R](
        rightQueue.poll,
        (r, m) => network.sendMessage(r, merge(Right(m)))
      )

    } yield (leftNetwork, rightNetwork)

  private def makeQueue[F[_]: Concurrent: ContextShift, A <: Agreement, M] =
    Resource.liftF {
      ConcurrentQueue.unbounded[F, MessageReceived[A#PKey, M]](None)
    }

  private class SplitNetwork[F[_]: Sync, A <: Agreement, M](
      poll: F[MessageReceived[A#PKey, M]],
      send: (A#PKey, M) => F[Unit]
  ) extends Network[F, A, M] {
    override def incomingMessages: Iterant[F, MessageReceived[A#PKey, M]] =
      Iterant.repeatEvalF(poll)

    def sendMessage(recipient: A#PKey, message: M) =
      send(recipient, message)
  }
}
