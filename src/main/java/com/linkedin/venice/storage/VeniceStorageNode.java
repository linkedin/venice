package com.linkedin.venice.storage;

/**
 * Generalized class for a storage node in Venice
 * All storage solutions need to extend this class.
 */
public abstract class VeniceStorageNode {

  private int nodeId = -1;

  /* Constructor required for successful compile */
  public VeniceStorageNode() { }

  public abstract int getNodeId();

  public abstract boolean containsPartition(int partitionId);
  public abstract boolean put(int partitionId, String key, Object value);

  public abstract Object get(int partitionId, String key);

  public abstract boolean delete(int partitionId, String key);

  public abstract boolean addPartition(int partition_id);

  public abstract VeniceStoragePartition removePartition(int partition_id);

}
