package com.linkedin.venice.store;

/**
 * Storage partition level config, which could be used to specify partition specific config when
 * initializing/opening the corresponding {@link AbstractStoragePartition}.
 */
public class StoragePartitionConfig {
  private final String storeName;
  private final int partitionId;
  private boolean deferredWrite;
  private boolean readOnly;
  private boolean writeOnlyConfig;

  public StoragePartitionConfig(String storeName, int partitionId) {
    this.storeName = storeName;
    this.partitionId = partitionId;
    this.deferredWrite = false;
    this.readOnly = false;
    this.writeOnlyConfig = true;
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

  public boolean isReadOnly() {
    return readOnly;
  }

  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  public void setWriteOnlyConfig(boolean writeOnly) {
    writeOnlyConfig = writeOnly;
  }

  public boolean isWriteOnlyConfig() {
    return writeOnlyConfig;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StoragePartitionConfig)) {
      return false;
    }

    StoragePartitionConfig that = (StoragePartitionConfig) o;

    if (partitionId != that.partitionId) {
      return false;
    }
    if (deferredWrite != that.deferredWrite) {
      return false;
    }
    if (readOnly != that.readOnly) {
      return false;
    }
    if (writeOnlyConfig != that.writeOnlyConfig) {
      return false;
    }
    return storeName.equals(that.storeName);
  }

  @Override
  public int hashCode() {
    int result = storeName.hashCode();
    result = 31 * result + partitionId;
    result = 31 * result + (deferredWrite ? 1 : 0);
    result = 31 * result + (readOnly ? 1 : 0);
    result = 31 * result + (writeOnlyConfig ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Store: " + storeName + ", partition id: " + partitionId + ", deferred-write: " + deferredWrite +
        ", read-only: " + readOnly + ", writeOnlyConfig: " + writeOnlyConfig;
  }
}
