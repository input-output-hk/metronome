package metronome.hotstuff

import metronome.core.Tagger

package object consensus {
  object ViewNumber extends Tagger[Long]
  type ViewNumber = ViewNumber.Tagged
}
