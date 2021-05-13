package io.iohk.metronome.hotstuff.service.codecs

import scodec.Codec
import scodec.codecs._
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.{
  Phase,
  VotingPhase,
  Agreement
}

trait DefaultConsensusCodecs[A <: Agreement] {
  import scodec.codecs.implicits._

  implicit def hashCodec: Codec[A#Hash]

  implicit val phaseCodec: Codec[VotingPhase] = {
    import Phase._
    mappedEnum(uint4, Prepare -> 1, PreCommit -> 2, Commit -> 3)
  }

  implicit val viewNumberCodec: Codec[ViewNumber] =
    Codec[Long].xmap(ViewNumber(_), identity)

  implicit val contentCodec: Codec[(VotingPhase, ViewNumber, A#Hash)] =
    phaseCodec ~~ viewNumberCodec ~~ hashCodec
}
