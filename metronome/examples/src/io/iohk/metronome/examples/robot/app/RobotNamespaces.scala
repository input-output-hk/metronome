package io.iohk.metronome.examples.robot.app

import io.iohk.metronome.rocksdb.RocksDBStore

object RobotNamespaces {
  private var registry: Vector[RocksDBStore.Namespace] = Vector.empty

  private def register(key: String): RocksDBStore.Namespace = {
    val ns = key.map(_.toByte)
    require(!registry.contains(ns))
    registry = registry :+ ns
    ns
  }

  def all: Seq[RocksDBStore.Namespace] = registry

  val Block           = register("block")
  val BlockMeta       = register("block-meta")
  val BlockToChildren = register("block-to-children")
  val ViewState       = register("view-state")
  val State           = register("state")
  val StateMeta       = register("state-meta")

}
