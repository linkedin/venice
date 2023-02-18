package com.linkedin.davinci.utils;

import com.linkedin.davinci.store.AbstractStorageEngine;


/**
 * This class maintains in-memory partition usage.
 * Triggered by size and/or time by #getUsage(), it will sync up with real disk usage.
 */
public class StoragePartitionDiskUsage {
  private final int partition;
  private final AbstractStorageEngine storageEngine;

  /**
   * This field indicates in memory partition usage since last syncing up with disk
   */
  private long inMemoryPartitionUsage;
  private long diskPartitionUsage;

  public StoragePartitionDiskUsage(int partition, AbstractStorageEngine storageEngine) {
    this.partition = partition;
    this.storageEngine = storageEngine;
    this.syncWithDB();
  }

  /**
   * Adds a usage size to the partition
   * @param recordSize
   * @return true if recordSize >= 0
   */
  public boolean add(long recordSize) {
    if (recordSize < 0) {
      return false;
    }
    synchronized (this) {
      this.inMemoryPartitionUsage += recordSize;
    }
    return true;
  }

  /**
   * When you want to check usage for a partition, use this method.
   * It will query syncing with real DB at a calculated frequency based on time and/or size trigger.
   * Generally calling this method should be quick
   *
   * @return the disk usage for this partition
   */
  public long getUsage() {
    synchronized (this) {
      return this.diskPartitionUsage + this.inMemoryPartitionUsage;
    }
  }

  /**
   * sync with real partition DB usage and reset in memory partition usage to be zero
   */
  public final void syncWithDB() {
    synchronized (this) {
      this.diskPartitionUsage = storageEngine.getPartitionSizeInBytes(this.partition);
      this.inMemoryPartitionUsage = 0;
    }
  }
}
