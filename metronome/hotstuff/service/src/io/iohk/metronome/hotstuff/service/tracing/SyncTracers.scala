package io.iohk.metronome.hotstuff.service.tracing

import io.iohk.metronome.hotstuff.consensus.basic.Agreement

case class SyncTracers[F[_], A <: Agreement]()
