package com.linkedin.davinci.store.rocksdb;

import com.linkedin.davinci.store.StorageEngineStats;
import java.io.File;
import java.util.function.LongSupplier;
import org.apache.commons.io.FileUtils;


public class RocksDBStorageEngineStats implements StorageEngineStats {
  private final String storeDbPath;
  private final LongSupplier getRMDSizeInBytes;
  private final LongSupplier getKeyCountEstimate;

  /**
   * The cached value will be refreshed by {@link #getStoreSizeInBytes()}.
   */
  private long cachedDiskUsage = 0;
  /**
   * The cached value will be refreshed by {@link #getRMDSizeInBytes()}.
   */
  private long cachedRMDDiskUsage = 0;

  public RocksDBStorageEngineStats(
      String storeDbPath,
      LongSupplier getRMDSizeInBytes,
      LongSupplier getKeyCountEstimate) {
    this.storeDbPath = storeDbPath;
    this.getRMDSizeInBytes = getRMDSizeInBytes;
    this.getKeyCountEstimate = getKeyCountEstimate;
  }

  @Override
  public long getStoreSizeInBytes() {
    File storeDbDir = new File(this.storeDbPath);
    if (storeDbDir.exists()) {
      /**
       * {@link FileUtils#sizeOf(File)} will throw {@link IllegalArgumentException} if the file/dir doesn't exist.
       */
      this.cachedDiskUsage = FileUtils.sizeOf(storeDbDir);
    } else {
      this.cachedDiskUsage = 0;
    }
    return this.cachedDiskUsage;
  }

  @Override
  public long getCachedStoreSizeInBytes() {
    return this.cachedDiskUsage;
  }

  @Override
  public long getRMDSizeInBytes() {
    this.cachedRMDDiskUsage = getRMDSizeInBytes.getAsLong();
    return this.cachedRMDDiskUsage;
  }

  @Override
  public long getCachedRMDSizeInBytes() {
    return this.cachedRMDDiskUsage;
  }

  @Override
  public long getKeyCountEstimate() {
    return this.getKeyCountEstimate.getAsLong();
  }

}
