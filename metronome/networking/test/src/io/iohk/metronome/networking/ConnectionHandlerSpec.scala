package io.iohk.metronome.networking

import cats.effect.Resource
import cats.effect.concurrent.Deferred
import io.iohk.metronome.networking.ConnectionHandler.{
  ConnectionAlreadyClosedException,
  FinishedConnection
}
import io.iohk.metronome.networking.ConnectionHandlerSpec.{
  buildHandlerResource,
  buildNConnections,
  _
}
import io.iohk.metronome.networking.EncryptedConnectionProvider.DecodingError
import io.iohk.metronome.networking.MockEncryptedConnectionProvider.MockEncryptedConnection
import io.iohk.metronome.networking.RemoteConnectionManagerTestUtils._
import monix.eval.Task
import monix.execution.Scheduler
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class ConnectionHandlerSpec extends AsyncFlatSpecLike with Matchers {
  implicit val testScheduler =
    Scheduler.fixedPool("ConnectionHandlerSpec", 16)
  implicit val timeOut = 5.seconds

  behavior of "ConnectionHandler"

  it should "register new connections" in customTestCaseResourceT(
    buildHandlerResource()
  ) { handler =>
    for {
      newConnection <- MockEncryptedConnection()
      _             <- handler.registerOutgoing(newConnection)
      connections   <- handler.getAllActiveConnections
    } yield {
      assert(connections.contains(newConnection.key))
    }
  }

  it should "send message to registered connection" in customTestCaseResourceT(
    buildHandlerResource()
  ) { handler =>
    for {
      newConnection <- MockEncryptedConnection()
      _             <- handler.registerOutgoing(newConnection)
      connections   <- handler.getAllActiveConnections
      sendResult    <- handler.sendMessage(newConnection.key, MessageA(1))
    } yield {
      assert(connections.contains(newConnection.key))
      assert(sendResult.isRight)
    }
  }

  it should "fail to send message to un-registered connection" in customTestCaseResourceT(
    buildHandlerResource()
  ) { handler =>
    for {
      newConnection <- MockEncryptedConnection()
      connections   <- handler.getAllActiveConnections
      sendResult    <- handler.sendMessage(newConnection.key, MessageA(1))
    } yield {
      assert(connections.isEmpty)
      assert(sendResult.isLeft)
      assert(
        sendResult.left.getOrElse(null) == ConnectionAlreadyClosedException(
          newConnection.key
        )
      )
    }
  }

  it should "fail to send message silently failed peer" in customTestCaseResourceT(
    buildHandlerResource()
  ) { handler =>
    for {
      newConnection <- MockEncryptedConnection()
      _             <- newConnection.closeRemoteWithoutInfo
      _             <- handler.registerOutgoing(newConnection)
      connections   <- handler.getAllActiveConnections
      sendResult    <- handler.sendMessage(newConnection.key, MessageA(1))
    } yield {
      assert(connections.contains(newConnection.key))
      assert(sendResult.isLeft)
      assert(
        sendResult.left.getOrElse(null) == ConnectionAlreadyClosedException(
          newConnection.key
        )
      )
    }
  }

  it should "not register and close duplicated connection" in customTestCaseResourceT(
    buildHandlerResource()
  ) { handler =>
    for {
      newConnection <- MockEncryptedConnection()
      duplicatedConnection <- MockEncryptedConnection(
        (newConnection.key, newConnection.address)
      )
      _                           <- handler.registerOutgoing(newConnection)
      connections                 <- handler.getAllActiveConnections
      _                           <- handler.registerOutgoing(duplicatedConnection)
      connectionsAfterDuplication <- handler.getAllActiveConnections
      closedAfterDuplication      <- duplicatedConnection.isClosed
    } yield {
      assert(connections.contains(newConnection.key))
      assert(connectionsAfterDuplication.contains(newConnection.key))
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
        handler.registerOutgoing(connection)
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
      newConnection  <- MockEncryptedConnection()
      _              <- handler.registerOutgoing(newConnection)
      numberOfActive <- handler.numberOfActiveConnections.waitFor(_ == 1)
      _              <- newConnection.pushRemoteEvent(None)
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
      newConnection  <- MockEncryptedConnection()
      _              <- handler.registerOutgoing(newConnection)
      numberOfActive <- handler.numberOfActiveConnections.waitFor(_ == 1)
      _              <- newConnection.pushRemoteEvent(Some(Left(DecodingError)))
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
      newConnection  <- MockEncryptedConnection()
      _              <- handler.registerOutgoing(newConnection)
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
        handler.registerOutgoing(connection)
      )
      maxNumberOfActiveConnections <- handler.numberOfActiveConnections
        .waitFor(numOfConnections =>
          numOfConnections == expectedNumberOfConnections
        )
      _ <- Task.traverse(connections) { encConnection =>
        encConnection.pushRemoteEvent(Some(Right(MessageA(1))))
      }
      receivedMessages <- handler.incomingMessages
        .take(expectedNumberOfConnections)
        .toListL
    } yield {

      val senders      = connections.map(_.key).toSet
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

  def buildHandlerResource(
      cb: FinishedConnection[Secp256k1Key] => Task[Unit] = _ => Task(())
  ): Resource[Task, ConnectionHandler[Task, Secp256k1Key, TestMessage]] = {
    ConnectionHandler
      .apply[Task, Secp256k1Key, TestMessage](cb)
  }

  def buildNConnections(n: Int)(implicit
      s: Scheduler
  ): Task[List[MockEncryptedConnection]] = {
    Task.traverse((0 until n).toList)(_ => MockEncryptedConnection())
  }

}
