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

  public VeniceStoreNode(int nodeId) {
    this.nodeId = nodeId;
  }

  public abstract int getNodeId();

  public abstract void put(String key, Object value);
  public abstract Object get(String key);

  // TODO: once internal partitioning is done, make this method protected
  public abstract void addPartitions(int partitionCount);

  protected abstract void addPartition();

}
