package com.linkedin.venice.store;

/**
 * Storage partition level config, which could be used to specify partition specific config when
 * initializing/opening the corresponding {@link AbstractStoragePartition}.
 */
public class StoragePartitionConfig {
  private final String storeName;
  private final int partitionId;
  private boolean deferredWrite;

  public StoragePartitionConfig(String storeName, int partitionId) {
    this.storeName = storeName;
    this.partitionId = partitionId;
    this.deferredWrite = false;
  }

  public String getStoreName() {
    return this.storeName;
  }

  public int getPartitionId() {
    return this.partitionId;
  }

  public void setDeferredWrite(boolean deferredWrite) {
    this.deferredWrite = deferredWrite;
  }

  public boolean isDeferredWrite() {
    return this.deferredWrite;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    StoragePartitionConfig that = (StoragePartitionConfig) o;

    if (partitionId != that.partitionId) return false;
    if (deferredWrite != that.deferredWrite) return false;
    return storeName.equals(that.storeName);

  }

  @Override
  public int hashCode() {
    int result = storeName.hashCode();
    result = 31 * result + partitionId;
    result = 31 * result + (deferredWrite ? 1 : 0);
    return result;
  }
}
