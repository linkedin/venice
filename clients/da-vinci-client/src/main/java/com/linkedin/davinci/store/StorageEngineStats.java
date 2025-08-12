package com.linkedin.davinci.store;

public interface StorageEngineStats {
  long getStoreSizeInBytes();

  long getCachedStoreSizeInBytes();

  long getRMDSizeInBytes();

  long getCachedRMDSizeInBytes();

  boolean hasMemorySpaceLeft();

  long getKeyCountEstimate();
}
