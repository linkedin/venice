package com.linkedin.venice.storage;

import com.linkedin.venice.message.VeniceMessage;

/**
 * Class for managing the storage system and its partitions
 * Created by clfung on 9/10/14.
 */
public abstract class VeniceStoreNode {

  private int nodeId = -1;

  /* Constructor required for successful compile */
  public VeniceStoreNode() { }

  public abstract int getNodeId();

  public abstract boolean containsPartition(int partitionId);
  public abstract boolean put(int partitionId, String key, Object value);

  public abstract Object get(int partitionId, String key);

  public abstract boolean delete(int partitionId, String key);

  public abstract boolean addPartition(int partition_id);

  public abstract VeniceStorePartition removePartition(int partition_id);

}
