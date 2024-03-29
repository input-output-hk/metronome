package io.iohk.metronome.examples.robot.app

import io.iohk.metronome.rocksdb.NamespaceRegistry

object RobotNamespaces extends NamespaceRegistry {
  val Block           = register("block")
  val BlockMeta       = register("block-meta")
  val BlockToChildren = register("block-to-children")
  val ViewState       = register("view-state")
  val State           = register("state")
  val StateMeta       = register("state-meta")
}
