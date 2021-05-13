package io.iohk.metronome.examples.robot.app

import io.iohk.metronome.rocksdb.RocksDBStore

object RobotNamespaces {
  private var registry: Vector[RocksDBStore.Namespace] = Vector.empty

  private def register(ns: RocksDBStore.Namespace): RocksDBStore.Namespace = {
    require(!registry.contains(ns))
    registry = registry :+ ns
    ns
  }

  def all: Seq[RocksDBStore.Namespace] = registry

}
