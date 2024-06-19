package com.linkedin.venice.blobtransfer;

import static com.linkedin.venice.store.rocksdb.RocksDBUtils.composePartitionDbDir;
import static com.linkedin.venice.store.rocksdb.RocksDBUtils.composeSnapshotDir;

import com.linkedin.venice.utils.Utils;


/**
 * POJO to hold the payload for blob transfer related informartion
 */
public class BlobTransferPayload {
  private final int partition;
  private final String storeNameAndVersion;
  private final String partitionDir;

  public BlobTransferPayload(String baseDir, String storeName, int version, int partition) {
    this.partition = partition;
    this.storeNameAndVersion = storeName + "_v" + version;
    this.partitionDir = composePartitionDbDir(baseDir, storeNameAndVersion, partition);
  }

  public String getPartitionDir() {
    return partitionDir;
  }

  public String getSnapshotDir() {
    return composeSnapshotDir(partitionDir);
  }

  public String getFullResourceName() {
    return Utils.getReplicaId(storeNameAndVersion, partition);
  }
}
