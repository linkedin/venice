package com.linkedin.davinci.store;

import com.linkedin.venice.stats.StatsErrorCode;


public class StorageEngineNoOpStats implements StorageEngineStats {
  public static final StorageEngineStats SINGLETON = new StorageEngineNoOpStats();

  private StorageEngineNoOpStats() {
  }

  @Override
  public long getStoreSizeInBytes() {
    // Not supported
    return StatsErrorCode.NOT_SUPPORTED.code;
  }

  @Override
  public long getCachedStoreSizeInBytes() {
    return 0;
  }

  @Override
  public long getRMDSizeInBytes() {
    return 0;
  }

  @Override
  public long getCachedRMDSizeInBytes() {
    return 0;
  }

  @Override
  public long getKeyCountEstimate() {
    return 0;
  }
}
