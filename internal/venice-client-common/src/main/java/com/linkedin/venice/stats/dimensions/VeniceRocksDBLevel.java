package com.linkedin.venice.stats.dimensions;

/**
 * Bucketed RocksDB LSM level where a Get was served from. LEVEL_2_AND_UP covers all
 * levels >= 2, matching RocksDB's GET_HIT_L2_AND_UP ticker.
 */
public enum VeniceRocksDBLevel implements VeniceDimensionInterface {
  LEVEL_0, LEVEL_1, LEVEL_2_AND_UP;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_ROCKSDB_LEVEL;
  }
}
