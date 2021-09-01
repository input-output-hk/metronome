package io.iohk.metronome.checkpointing.app

import monix.eval.Task
import io.iohk.metronome.crypto.ECKeyPair
import io.circe.Json
import java.security.SecureRandom

/** Generate an ECDSA key pair and print it on the console.
  *
  * Using JSON format so that it's obvious which part is what.
  * We can use `jq` to parse on the command line if necessary.
  */
object CheckpointingKeyGen {
  def format(pair: ECKeyPair): String = {
    val json = Json.obj(
      "publicKey"  -> Json.fromString(pair.pub.bytes.toHex),
      "privateKey" -> Json.fromString(pair.prv.bytes.toHex)
    )
    json.spaces2
  }

  def print(pair: ECKeyPair): Task[Unit] =
    Task(println(format(pair)))

  def generateAndPrint: Task[Unit] =
    for {
      rng <- Task(new java.security.SecureRandom())
      keys = ECKeyPair.generate(rng)
      _ <- print(keys)
    } yield ()
}
