package io.iohk.metronome.examples.robot.codecs

import scodec.Codec
import scodec.codecs._
import io.iohk.metronome.crypto.hash.Hash
import io.iohk.metronome.hotstuff.consensus.basic.{Phase, VotingPhase}
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import scodec.bits.ByteVector

trait RobotConsensusCodecs {
  import scodec.codecs.implicits._

  implicit val hashCodec: Codec[Hash] =
    Codec[ByteVector].xmap(Hash(_), identity)

  implicit val phaseCodec: Codec[VotingPhase] = {
    import Phase._
    mappedEnum(uint4, Prepare -> 1, PreCommit -> 2, Commit -> 3)
  }

  implicit val viewNumberCodec: Codec[ViewNumber] =
    Codec[Long].xmap(ViewNumber(_), identity)

  implicit val contentCodec: Codec[(VotingPhase, ViewNumber, Hash)] =
    phaseCodec ~~ viewNumberCodec ~~ hashCodec
}
