package metronome.core

import shapeless.tag, tag.@@

/** Helper class to make it easier to tag raw types such as BitVector
  * to specializations so that the compiler can help make sure we are
  * passign the right values to methods.
  *
  * ```
  * object MyType extends Tagger[ByteVector]
  * type MyType = MyType.Tagged
  *
  * val myThing: MyType = MyType(ByteVector.empty)
  * ```
  */
protected[metronome] trait Tagger[U] {
  trait Tag
  type Tagged = U @@ Tag
  def apply(underlying: U): Tagged =
    tag[Tag][U](underlying)
}
