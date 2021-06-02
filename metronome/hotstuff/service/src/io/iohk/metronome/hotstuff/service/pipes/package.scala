package io.iohk.metronome.hotstuff.service

import io.iohk.metronome.core.Pipe
import io.iohk.metronome.hotstuff.consensus.basic.Agreement

package object pipes {

  /** Communication pipe with the block synchronization and validation component. */
  type SyncPipe[F[_], A <: Agreement] =
    Pipe[F, SyncPipe.Request[A], SyncPipe.Response[A]]
}
