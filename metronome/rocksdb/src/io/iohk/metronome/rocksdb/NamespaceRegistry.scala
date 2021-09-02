package io.iohk.metronome.rocksdb

trait NamespaceRegistry {
  private var registry: Vector[RocksDBStore.Namespace] = Vector.empty

  protected def register(key: String): RocksDBStore.Namespace = {
    val ns = key.map(_.toByte)
    require(!registry.contains(ns))
    registry = registry :+ ns
    ns
  }

  def all: Seq[RocksDBStore.Namespace] = registry
}
