package io.iohk.metronome.core

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
trait Tagger[U] {
  trait Tag
  type Tagged = U @@ Tag
  def apply(underlying: U): Tagged =
    tag[Tag][U](underlying)
}

/** Helper class to tag not a specific raw type, but to apply a common tag to any type.
  *
  * ```
  * object Validated extends GenericTagger
  * type Validated[U] = Validated.Tagged[U]
  * ```
  */
trait GenericTagger {
  trait Tag
  type Tagged[U] = U @@ Tag

  def apply[U](underlying: U): Tagged[U] =
    tag[Tag][U](underlying)
}
