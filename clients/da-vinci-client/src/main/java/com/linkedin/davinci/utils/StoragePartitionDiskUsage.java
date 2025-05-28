package com.linkedin.davinci.utils;

import java.util.function.LongSupplier;


/**
 * This class maintains in-memory partition usage.
 * Triggered by size and/or time by #getUsage(), it will sync up with real disk usage.
 */
public class StoragePartitionDiskUsage {
  private final LongSupplier usageGetter;

  /**
   * Disk usage + memory usage since last sync with the disk
   */
  private volatile long combinedPartitionUsage = 0;

  public StoragePartitionDiskUsage(LongSupplier usageGetter) {
    this.usageGetter = usageGetter;
    this.syncWithDB();
  }

  /**
   * Adds a usage size to the partition
   */
  public void add(long recordSize) {
    if (recordSize > 0) {
      synchronized (this) {
        this.combinedPartitionUsage += recordSize;
      }
    }
  }

  /**
   * @return the disk usage for this partition
   */
  public long getUsage() {
    return this.combinedPartitionUsage;
  }

  /**
   * sync with real partition DB usage and reset in memory partition usage to be zero
   */
  public final synchronized void syncWithDB() {
    this.combinedPartitionUsage = usageGetter.getAsLong();
  }
}
