package com.linkedin.davinci.store;

import com.linkedin.venice.utils.Utils;
import java.util.Objects;


/**
 * Storage partition level config, which could be used to specify partition specific config when
 * initializing/opening the corresponding {@link AbstractStoragePartition}.
 */
public class StoragePartitionConfig {
  private final String storeName;
  private final int partitionId;
  /**
   * Refer {@link com.linkedin.davinci.store.rocksdb.RocksDBStoragePartition#deferredWrite}
   */
  private boolean deferredWrite;
  private boolean readOnly;
  private boolean writeOnlyConfig;
  private boolean readWriteLeaderForDefaultCF;
  private boolean readWriteLeaderForRMDCF;
  private final boolean blobTransferInProgress;

  public StoragePartitionConfig(String storeName, int partitionId) {
    this(storeName, partitionId, false);
  }

  public StoragePartitionConfig(String storeName, int partitionId, boolean isBlobTransferInProgress) {
    this.storeName = storeName;
    this.partitionId = partitionId;
    this.deferredWrite = false;
    this.readOnly = false;
    this.writeOnlyConfig = true;
    this.readWriteLeaderForDefaultCF = false;
    this.readWriteLeaderForRMDCF = false;
    this.blobTransferInProgress = isBlobTransferInProgress;
  }

  public String getStoreName() {
    return this.storeName;
  }

  public int getPartitionId() {
    return this.partitionId;
  }

  public boolean isBlobTransferInProgress() {
    return blobTransferInProgress;
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

  public boolean isReadWriteLeaderForDefaultCF() {
    return readWriteLeaderForDefaultCF;
  }

  public void setReadWriteLeaderForDefaultCF(boolean readWriteLeaderForDefaultCF) {
    this.readWriteLeaderForDefaultCF = readWriteLeaderForDefaultCF;
  }

  public boolean isReadWriteLeaderForRMDCF() {
    return readWriteLeaderForRMDCF;
  }

  public void setReadWriteLeaderForRMDCF(boolean readWriteLeaderForRMDCF) {
    this.readWriteLeaderForRMDCF = readWriteLeaderForRMDCF;
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
        && writeOnlyConfig == that.writeOnlyConfig && storeName.equals(that.storeName)
        && readWriteLeaderForDefaultCF == that.readWriteLeaderForDefaultCF
        && readWriteLeaderForRMDCF == that.readWriteLeaderForRMDCF;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        storeName,
        partitionId,
        deferredWrite,
        readOnly,
        writeOnlyConfig,
        readWriteLeaderForDefaultCF,
        readWriteLeaderForRMDCF);
  }

  @Override
  public String toString() {
    String toStringResult =
        "Replica: " + Utils.getReplicaId(storeName, partitionId) + ", deferred-write: " + deferredWrite
            + ", read-only: " + readOnly + ", write-only: " + writeOnlyConfig + ", read-write leader for default CF: "
            + readWriteLeaderForDefaultCF + ", read-write leader for RMD CF: " + readWriteLeaderForRMDCF;

    if (blobTransferInProgress) {
      toStringResult += ", blob transfer in progress: true.";
    }

    return toStringResult;
  }
}
