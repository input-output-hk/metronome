package io.iohk.metronome.hotstuff.service.tracing

import io.iohk.metronome.hotstuff.consensus.basic.Agreement

sealed trait ConsensusEvent[A <: Agreement]
