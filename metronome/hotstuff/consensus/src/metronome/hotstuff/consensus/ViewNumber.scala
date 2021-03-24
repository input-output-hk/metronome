package metronome.hotstuff.consensus

import metronome.core.Tagger

object ViewNumber extends Tagger[Long] {
  implicit class Ops(val vn: ViewNumber) extends AnyVal {
    def next: ViewNumber = ViewNumber(vn + 1)
    def prev: ViewNumber = ViewNumber(vn - 1)
  }

  implicit val ord: Ordering[ViewNumber] =
    Ordering.by(identity[Long])
}
