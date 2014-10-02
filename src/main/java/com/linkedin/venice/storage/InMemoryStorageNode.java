package com.linkedin.venice.storage;

import com.linkedin.venice.metadata.NodeCache;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * An in-memory hashmap implementation of a Venice storage node
 */
public class InMemoryStorageNode extends VeniceStorageNode {

  static final Logger logger = Logger.getLogger(InMemoryStorageNode.class.getName());

  private int nodeId = -1;

  // Map which stores the partitions and their associated ids
  private Map<Integer, InMemoryStoragePartition> partitions = new HashMap<Integer, InMemoryStoragePartition>();
  private static NodeCache nodeCache;

  // number of partitions in the store
  private static int partitionCount = -1;

  public InMemoryStorageNode(int nodeId) {

    // register current nodeId
    this.nodeId = nodeId;

    // create static instance of nodeCache
    nodeCache = NodeCache.getInstance();

  }

  @Override
  /**
   * Returns the nodeId of this given node.
   */
  public int getNodeId() {
    return nodeId;
  }

  @Override
  /**
   * Return true or false based on whether a given partition exists within this node.
   * @param partitionId - The partition to look for
   * @return True/False, does the partition exist on this node
   * */
  public boolean containsPartition(int partitionId) {
    return partitions.containsKey(partitionId);
  }

  /**
   * Adds a partitionId to the current Store
   * @param store_id - id of partition to add
   */
  @Override
  public boolean addPartition(int store_id) {

    if (partitions.containsKey(store_id)) {
      logger.error("Error on nodeId: " + nodeId +
          " attempted to add a partition with an id that already exists: " + store_id);
      return false;
    }

    partitions.put(store_id, new InMemoryStoragePartition(store_id));
    return true;

  }

  /**
   * Removes and returns a partitionId to the current Store
   * @param store_id - id of partition to retrieve and remove
   */
  @Override
  public InMemoryStoragePartition removePartition(int store_id) {

    if (!partitions.containsKey(store_id)) {
      logger.error("Error on nodeId: " + nodeId +
          " attempted to remove a partition with an id that does not exist: " + store_id);
      return null;
    }

    InMemoryStoragePartition toReturn = partitions.get(store_id);
    partitions.remove(store_id);

    return toReturn;

  }

  /**
   * Add a key-value pair to storage.
   * @param partitionId - The partition to add to: should map directly to Kafka
   * @param key - The key of the data in the KV pair
   * @param value - The value of the data in the KV pair
   * @return true, if the put was successful
   */
  @Override
  public boolean put(int partitionId, String key, Object value) {

    if (!partitions.containsKey(partitionId)) {
      logger.warn("PartitionId " + partitionId + " does not exist on NodeId " + nodeId);
      return false;
    }

    InMemoryStoragePartition partition = partitions.get(partitionId);

    logger.info("Running put on node: " + nodeId + " partition: " + partitionId);
    partition.put(key, value);

    return true;

  }

  /**
   * Get a value from storage.
   * @param partitionId - The partition to read from: should map directly to Kafka
   * @param key - The key of the data to be queried
   */
  @Override
  public Object get(int partitionId, String key) {

    if (!partitions.containsKey(partitionId)) {
      logger.error("Cannot find partition id: " + partitionId);
      return null;
    }

    return partitions.get(partitionId).get(key);

  }

  /**
   * Remove a value from storage.
   * @param partitionId - The partition to read from: should map directly to Kafka
   * @param key - The key of the data to be deleted
   */
  @Override
  public boolean delete(int partitionId, String key) {

    if (!partitions.containsKey(partitionId)) {
      logger.error("Cannot find partition id: " + partitionId);
      return false;
    }

    logger.info("Run a delete on node: " + nodeId + " partition: " + partitionId);
    partitions.get(partitionId).delete(key);
    return true;

  }

}
