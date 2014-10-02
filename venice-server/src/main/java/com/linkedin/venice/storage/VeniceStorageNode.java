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

  public abstract void put(int partitionId, String key, Object value) throws VeniceStorageException;

  public abstract Object get(int partitionId, String key) throws VeniceStorageException;

  public abstract void delete(int partitionId, String key) throws VeniceStorageException;

  public abstract void addPartition(int partition_id) throws VeniceStorageException;

  public abstract VeniceStoragePartition removePartition(int partition_id) throws VeniceStorageException;

}
