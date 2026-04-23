package com.linkedin.venice.stats.dimensions;

/** RocksDB block cache component type. */
public enum VeniceRocksDBBlockCacheComponent implements VeniceDimensionInterface {
  INDEX, FILTER, DATA, COMPRESSION_DICT;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_ROCKSDB_BLOCK_CACHE_COMPONENT;
  }
}
