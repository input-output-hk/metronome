package io.iohk.metronome.networking

import monix.execution.Scheduler
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import RemoteConnectionManagerTestUtils._
import cats.effect.Resource
import io.iohk.metronome.networking.ConnectionHandler.HandledConnection
import io.iohk.metronome.networking.ConnectionHandlerSpec.{
  buildHandlerResource,
  buildNConnections,
  newHandledConnection
}
import io.iohk.metronome.networking.MockEncryptedConnectionProvider.MockEncryptedConnection
import monix.eval.Task
class ConnectionHandlerSpec extends AsyncFlatSpecLike with Matchers {
  implicit val testScheduler =
    Scheduler.fixedPool("RemoteConnectionManagerUtSpec", 16)
  implicit val timeOut = 5.seconds

  behavior of "RemoteConnectionManagerWithMockProvider"

  it should "register new connections" in customTestCaseT {
    for {
      handlerAndRelease <- buildHandlerResource().allocated
      (handler, release) = handlerAndRelease
      handledConnection1 <- newHandledConnection
      _                  <- handler.registerIfAbsent(handledConnection1)
      connections        <- handler.getAllActiveConnections
    } yield {
      assert(connections.contains(handledConnection1.key))
    }
  }

  it should "clear new connections when released" in customTestCaseT {
    val expectedNumberOfConnections = 4
    for {
      handlerAndRelease <- buildHandlerResource().allocated
      (handler, release) = handlerAndRelease
      connections <- buildNConnections(expectedNumberOfConnections)
      _ <- Task.traverse(connections)(connection =>
        handler.registerIfAbsent(connection)
      )
      maxNumberOfActiveConnections <- handler.numberOfActiveConnections
        .restartUntil(numberOfConnections =>
          numberOfConnections == expectedNumberOfConnections
        )
        .timeout(timeOut)
      _ <- release
      connectionsAfterClose <- handler.getAllActiveConnections
        .restartUntil(con => con.isEmpty)
        .timeout(timeOut)
    } yield {
      assert(maxNumberOfActiveConnections == expectedNumberOfConnections)
      assert(connectionsAfterClose.isEmpty)
    }
  }
}

object ConnectionHandlerSpec {
  def buildHandler(): Task[ConnectionHandler[Task, Secp256k1Key, TestMessage]] =
    ConnectionHandler[Task, Secp256k1Key, TestMessage](_ => Task(()))

  def buildHandlerResource(
      cb: HandledConnection[Task, Secp256k1Key, TestMessage] => Task[Unit] =
        _ => Task(())
  ): Resource[Task, ConnectionHandler[Task, Secp256k1Key, TestMessage]] = {
    ConnectionHandler
      .connectionHandlerResource[Task, Secp256k1Key, TestMessage](cb)
  }

  def newHandledConnection(implicit
      s: Scheduler
  ): Task[HandledConnection[Task, Secp256k1Key, TestMessage]] = {
    for {
      enc <- MockEncryptedConnection()
    } yield HandledConnection.outgoing(enc)
  }

  def buildNConnections(n: Int)(implicit
      s: Scheduler
  ): Task[List[HandledConnection[Task, Secp256k1Key, TestMessage]]] = {
    Task.traverse((0 until n).toList)(_ => newHandledConnection)
  }

}
