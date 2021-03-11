package metronome.hotstuff.consensus

case class ViewNumber(val value: Long) extends AnyVal {
  def `+`(i: Int): ViewNumber = ViewNumber(value + i)
  def `-`(i: Int): ViewNumber = ViewNumber(value - i)
}
