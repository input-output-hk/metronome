package io.iohk.metronome.hotstuff.consensus

import io.iohk.metronome.core.Tagger
import cats.kernel.Order

object ViewNumber extends Tagger[Long] {
  implicit class Ops(val vn: ViewNumber) extends AnyVal {
    def next: ViewNumber = ViewNumber(vn + 1)
    def prev: ViewNumber = ViewNumber(vn - 1)
  }

  implicit val ord: Ordering[ViewNumber] =
    Ordering.by(identity[Long])

  implicit val order: Order[ViewNumber] =
    Order.fromOrdering(ord)
}
