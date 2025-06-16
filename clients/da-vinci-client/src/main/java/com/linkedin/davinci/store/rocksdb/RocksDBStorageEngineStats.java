package com.linkedin.davinci.store.rocksdb;

import com.linkedin.davinci.store.StorageEngineStats;
import java.io.File;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.apache.commons.io.FileUtils;


public class RocksDBStorageEngineStats implements StorageEngineStats {
  private final String storeDbPath;
  private final LongSupplier getRMDSizeInBytes;
  private final LongSupplier getDuplicateKeyCountEstimate;
  private final LongSupplier getKeyCountEstimate;
  private final BooleanSupplier hasMemorySpaceLeft;

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
      LongSupplier getDuplicateKeyCountEstimate,
      LongSupplier getKeyCountEstimate,
      BooleanSupplier hasMemorySpaceLeft) {
    this.storeDbPath = storeDbPath;
    this.getRMDSizeInBytes = getRMDSizeInBytes;
    this.getDuplicateKeyCountEstimate = getDuplicateKeyCountEstimate;
    this.getKeyCountEstimate = getKeyCountEstimate;
    this.hasMemorySpaceLeft = hasMemorySpaceLeft;
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
  public boolean hasMemorySpaceLeft() {
    return this.hasMemorySpaceLeft.getAsBoolean();
  }

  @Override
  public long getDuplicateKeyCountEstimate() {
    return this.getDuplicateKeyCountEstimate.getAsLong();
  }

  @Override
  public long getKeyCountEstimate() {
    return this.getKeyCountEstimate.getAsLong();
  }

}
