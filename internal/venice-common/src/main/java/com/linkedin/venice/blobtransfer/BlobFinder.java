package com.linkedin.venice.blobtransfer;

public interface BlobFinder extends AutoCloseable {
  /**
   * This method will look through the partitions for the store and version provided until it finds the partition
   * requested, it will then return the URL of the instances that are ready to serve in the partition.
   */
  BlobPeersDiscoveryResponse discoverBlobPeers(String storeName, int version, int partitionId);

}
