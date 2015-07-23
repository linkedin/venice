package com.linkedin.venice.config;

/**
 * Class representing basic store information such as Store Name, Number of store partitions & Number of replicas.
 */
public class VeniceStorePartitionInformation {

  private final String storeName;
  private final int replicationFactor;
  private final int partitionsCount;


  public VeniceStorePartitionInformation(String storeName, int partitionsCount, int replicationFactor) {
    this.storeName = storeName;
    this.replicationFactor = replicationFactor;
    this.partitionsCount = partitionsCount;
  }

  public String getStoreName() {
    return storeName;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public int getPartitionsCount() {
    return partitionsCount;
  }
}
