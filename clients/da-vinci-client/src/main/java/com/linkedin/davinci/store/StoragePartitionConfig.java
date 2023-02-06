package com.linkedin.davinci.store;

import java.util.Objects;


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
    if (readOnly) {
      setWriteOnlyConfig(false);
    }
  }

  public boolean isWriteOnlyConfig() {
    return writeOnlyConfig;
  }

  public void setWriteOnlyConfig(boolean writeOnly) {
    writeOnlyConfig = writeOnly;
    if (writeOnly) {
      setReadOnly(false);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StoragePartitionConfig that = (StoragePartitionConfig) o;
    return partitionId == that.partitionId && deferredWrite == that.deferredWrite && readOnly == that.readOnly
        && writeOnlyConfig == that.writeOnlyConfig && storeName.equals(that.storeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storeName, partitionId, deferredWrite, readOnly, writeOnlyConfig);
  }

  @Override
  public String toString() {
    return "Store: " + storeName + ", partition id: " + partitionId + ", deferred-write: " + deferredWrite
        + ", read-only: " + readOnly + ", write-only: " + writeOnlyConfig;
  }
}
