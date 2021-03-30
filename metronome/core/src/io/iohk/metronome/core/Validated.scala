package io.iohk.metronome.core

/** Can be used to tag any particular type as validated, for example:
  *
  * ```
  * def validateBlock(block: Block): Either[Error, Validated[Block]]
  * def storeBlock(block: Validated[Block])
  * ```
  *
  * It's a bit more lightweight than opting into the `ValidatedNel` from `cats`,
  * mostly just serves as control that the right methods have been called in a
  * pipeline.
  */
object Validated extends GenericTagger
