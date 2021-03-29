package io.iohk.metronome.networking

import monix.execution.Scheduler
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import RemoteConnectionManagerTestUtils._
import cats.effect.Resource
import cats.effect.concurrent.Deferred
import io.iohk.metronome.networking.ConnectionHandler.{
  ConnectionAlreadyClosedException,
  HandledConnection
}
import io.iohk.metronome.networking.ConnectionHandlerSpec.{
  buildHandlerResource,
  buildNConnections,
  newHandledConnection
}
import io.iohk.metronome.networking.MockEncryptedConnectionProvider.MockEncryptedConnection
import monix.eval.Task
import ConnectionHandlerSpec._
import io.iohk.metronome.networking.EncryptedConnectionProvider.DecodingError
import io.iohk.metronome.networking.RemoteConnectionManagerWithMockProviderSpec.fakeLocalAddress

import java.net.InetSocketAddress
import io.iohk.tracer.Tracer

class ConnectionHandlerSpec extends AsyncFlatSpecLike with Matchers {
  implicit val testScheduler =
    Scheduler.fixedPool("ConnectionHandlerSpec", 16)
  implicit val timeOut = 5.seconds

  behavior of "ConnectionHandler"

  it should "register new connections" in customTestCaseResourceT(
    buildHandlerResource()
  ) { handler =>
    for {
      handledConnection1 <- newHandledConnection()
      _                  <- handler.registerOrClose(handledConnection1._1)
      connections        <- handler.getAllActiveConnections
    } yield {
      assert(connections.contains(handledConnection1._1.key))
    }
  }

  it should "send message to registered connection" in customTestCaseResourceT(
    buildHandlerResource()
  ) { handler =>
    for {
      handledConnection1 <- newHandledConnection()
      _                  <- handler.registerOrClose(handledConnection1._1)
      connections        <- handler.getAllActiveConnections
      sendResult         <- handler.sendMessage(handledConnection1._1.key, MessageA(1))
    } yield {
      assert(connections.contains(handledConnection1._1.key))
      assert(sendResult.isRight)
    }
  }

  it should "fail to send message to un-registered connection" in customTestCaseResourceT(
    buildHandlerResource()
  ) { handler =>
    for {
      handledConnection1 <- newHandledConnection()
      connections        <- handler.getAllActiveConnections
      sendResult         <- handler.sendMessage(handledConnection1._1.key, MessageA(1))
    } yield {
      assert(connections.isEmpty)
      assert(sendResult.isLeft)
      assert(
        sendResult.left.getOrElse(null) == ConnectionAlreadyClosedException(
          handledConnection1._1.key
        )
      )
    }
  }

  it should "fail to send message silently failed peer" in customTestCaseResourceT(
    buildHandlerResource()
  ) { handler =>
    for {
      handledConnection1 <- newHandledConnection()
      (handled, underLaying) = handledConnection1
      _           <- underLaying.closeRemoteWithoutInfo
      _           <- handler.registerOrClose(handledConnection1._1)
      connections <- handler.getAllActiveConnections
      sendResult  <- handler.sendMessage(handledConnection1._1.key, MessageA(1))
    } yield {
      assert(connections.contains(handledConnection1._1.key))
      assert(sendResult.isLeft)
      assert(
        sendResult.left.getOrElse(null) == ConnectionAlreadyClosedException(
          handledConnection1._1.key
        )
      )
    }
  }

  it should "not register and close duplicated connection" in customTestCaseResourceT(
    buildHandlerResource()
  ) { handler =>
    for {
      handledConnection <- newHandledConnection()
      duplicatedConnection <- newHandledConnection(remotePeerInfo =
        (handledConnection._1.key, handledConnection._1.serverAddress)
      )
      (handled, underlyingEncrypted) = handledConnection
      _                           <- handler.registerOrClose(handled)
      connections                 <- handler.getAllActiveConnections
      _                           <- handler.registerOrClose(duplicatedConnection._1)
      connectionsAfterDuplication <- handler.getAllActiveConnections
      closedAfterDuplication      <- duplicatedConnection._2.isClosed
    } yield {
      assert(connections.contains(handled.key))
      assert(connectionsAfterDuplication.contains(handled.key))
      assert(closedAfterDuplication)

    }
  }

  it should "close all connections in background when released" in customTestCaseT {
    val expectedNumberOfConnections = 4
    for {
      handlerAndRelease <- buildHandlerResource().allocated
      (handler, release) = handlerAndRelease
      connections <- buildNConnections(expectedNumberOfConnections)
      _ <- Task.traverse(connections)(connection =>
        handler.registerOrClose(connection._1)
      )
      maxNumberOfActiveConnections <- handler.numberOfActiveConnections
        .waitFor(numOfConnections =>
          numOfConnections == expectedNumberOfConnections
        )

      _ <- release
      connectionsAfterClose <- handler.getAllActiveConnections.waitFor(
        connections => connections.isEmpty
      )
    } yield {
      assert(maxNumberOfActiveConnections == expectedNumberOfConnections)
      assert(connectionsAfterClose.isEmpty)
    }
  }

  it should "call provided callback when connection is closed" in customTestCaseT {
    for {
      cb                <- Deferred.tryable[Task, Unit]
      handlerAndRelease <- buildHandlerResource(_ => cb.complete(())).allocated
      (handler, release) = handlerAndRelease
      connection <- newHandledConnection()
      (handledConnection, underlyingEncrypted) = connection
      _              <- handler.registerOrClose(handledConnection)
      numberOfActive <- handler.numberOfActiveConnections.waitFor(_ == 1)
      _              <- underlyingEncrypted.pushRemoteEvent(None)
      numberOfActiveAfterDisconnect <- handler.numberOfActiveConnections
        .waitFor(_ == 0)
      callbackCompleted <- cb.tryGet.waitFor(_.isDefined)
      _                 <- release
    } yield {
      assert(numberOfActive == 1)
      assert(numberOfActiveAfterDisconnect == 0)
      assert(callbackCompleted.isDefined)
    }
  }

  it should "call provided callback and close connection in case of error" in customTestCaseT {
    for {
      cb                <- Deferred.tryable[Task, Unit]
      handlerAndRelease <- buildHandlerResource(_ => cb.complete(())).allocated
      (handler, release) = handlerAndRelease
      connection <- newHandledConnection()
      (handledConnection, underlyingEncrypted) = connection
      _              <- handler.registerOrClose(handledConnection)
      numberOfActive <- handler.numberOfActiveConnections.waitFor(_ == 1)
      _              <- underlyingEncrypted.pushRemoteEvent(Some(Left(DecodingError)))
      numberOfActiveAfterError <- handler.numberOfActiveConnections
        .waitFor(_ == 0)
      callbackCompleted <- cb.tryGet.waitFor(_.isDefined)
      _                 <- release
    } yield {
      assert(numberOfActive == 1)
      assert(numberOfActiveAfterError == 0)
      assert(callbackCompleted.isDefined)
    }
  }

  it should "try not to call callback in case of closing manager" in customTestCaseT {
    for {
      cb                <- Deferred.tryable[Task, Unit]
      handlerAndRelease <- buildHandlerResource(_ => cb.complete(())).allocated
      (handler, release) = handlerAndRelease
      connection <- newHandledConnection()
      (handledConnection, underlyingEncrypted) = connection
      _              <- handler.registerOrClose(handledConnection)
      numberOfActive <- handler.numberOfActiveConnections.waitFor(_ == 1)
      _              <- release
      numberOfActiveAfterDisconnect <- handler.numberOfActiveConnections
        .waitFor(_ == 0)
      callbackCompleted <- cb.tryGet.waitFor(_.isDefined).attempt
    } yield {
      assert(numberOfActive == 1)
      assert(numberOfActiveAfterDisconnect == 0)
      assert(callbackCompleted.isLeft)
    }
  }

  it should "multiplex messages from all open channels" in customTestCaseResourceT(
    buildHandlerResource()
  ) { handler =>
    val expectedNumberOfConnections = 4
    for {
      connections <- buildNConnections(expectedNumberOfConnections)
      _ <- Task.traverse(connections)(connection =>
        handler.registerOrClose(connection._1)
      )
      maxNumberOfActiveConnections <- handler.numberOfActiveConnections
        .waitFor(numOfConnections =>
          numOfConnections == expectedNumberOfConnections
        )
      _ <- Task.traverse(connections) { case (_, encConnection) =>
        encConnection.pushRemoteEvent(Some(Right(MessageA(1))))
      }
      receivedMessages <- handler.incomingMessages
        .take(expectedNumberOfConnections)
        .toListL
    } yield {

      val senders      = connections.map(_._1.key).toSet
      val receivedFrom = receivedMessages.map(_.from).toSet
      assert(receivedMessages.size == expectedNumberOfConnections)
      assert(maxNumberOfActiveConnections == expectedNumberOfConnections)
      assert(
        senders.intersect(receivedFrom).size == expectedNumberOfConnections
      )
    }
  }

}

object ConnectionHandlerSpec {
  implicit class TaskOps[A](task: Task[A]) {
    def waitFor(condition: A => Boolean)(implicit timeOut: FiniteDuration) = {
      task.restartUntil(condition).timeout(timeOut)
    }
  }

  implicit val tracers: NetworkTracers[Task, Secp256k1Key, TestMessage] =
    NetworkTracers(Tracer.noOpTracer)

  def buildHandlerResource(
      cb: HandledConnection[Task, Secp256k1Key, TestMessage] => Task[Unit] =
        _ => Task(())
  ): Resource[Task, ConnectionHandler[Task, Secp256k1Key, TestMessage]] = {
    ConnectionHandler
      .apply[Task, Secp256k1Key, TestMessage](cb)
  }

  def newHandledConnection(
      remotePeerInfo: (Secp256k1Key, InetSocketAddress) =
        (Secp256k1Key.getFakeRandomKey, fakeLocalAddress)
  )(implicit
      s: Scheduler
  ): Task[
    (
        HandledConnection[Task, Secp256k1Key, TestMessage],
        MockEncryptedConnection
    )
  ] = {
    for {
      enc <- MockEncryptedConnection(remotePeerInfo)
    } yield (HandledConnection.outgoing(enc), enc)
  }

  def buildNConnections(n: Int)(implicit
      s: Scheduler
  ): Task[List[
    (
        HandledConnection[Task, Secp256k1Key, TestMessage],
        MockEncryptedConnection
    )
  ]] = {
    Task.traverse((0 until n).toList)(_ => newHandledConnection())
  }

}
