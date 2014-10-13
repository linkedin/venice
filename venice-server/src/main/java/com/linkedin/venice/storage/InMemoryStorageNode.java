package com.linkedin.venice.storage;

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

  // number of partitions in the store
  private static int partitionCount = -1;

  public InMemoryStorageNode(int nodeId) {

    // register current nodeId
    this.nodeId = nodeId;

  }

  /**
   * Returns the nodeId of this given node.
   */
  @Override
  public int getNodeId() {
    return nodeId;
  }

  /**
   * Return true or false based on whether a given partition exists within this node.
   * @param partitionId - The partition to look for
   * @return True/False, does the partition exist on this node
   * */
  @Override
  public boolean containsPartition(int partitionId) {
    return partitions.containsKey(partitionId);
  }

  /**
   * Adds a partitionId to the current Store
   * @param partitionId - id of partition to add
   * @throws VeniceStorageException, if the added partitionId already exists
   */
  @Override
  public void addStoragePartition(int partitionId) throws VeniceStorageException {

    if (partitions.containsKey(partitionId)) {
      throw new VeniceStorageException("Error on nodeId: " + nodeId +
          " attempted to add a partition with an id that already exists: " + partitionId);
    }

    partitions.put(partitionId, new InMemoryStoragePartition(partitionId));

  }

  /**
   * Removes and returns a partitionId to the current Store
   * @param partitionId - id of partition to retrieve and remove
   * @return The partition object removed by this operation
   * @throws VeniceStorageException, if the removed partition does not exist
   */
  @Override
  public InMemoryStoragePartition removePartition(int partitionId) throws VeniceStorageException {

    if (!partitions.containsKey(partitionId)) {
      throw new VeniceStorageException("Error on nodeId: " + nodeId +
          ", attempted to remove a partition with an id that does not exist: " + partitionId);
    }

    InMemoryStoragePartition toReturn = partitions.get(partitionId);
    partitions.remove(partitionId);

    return toReturn;

  }

  /**
   * Add a key-value pair to storage.
   * @param partitionId - The partition to add to: should map directly to Kafka
   * @param key - The key of the data in the KV pair
   * @param value - The value of the data in the KV pair
   * @throws VeniceStorageException if the specified partitionId does not exist on this node
   */
  @Override
  public void put(int partitionId, String key, Object value) throws VeniceStorageException {

    if (!partitions.containsKey(partitionId)) {
      throw new VeniceStorageException("On put: PartitionId " + partitionId + " does not exist on nodeId " + nodeId);
    }

    InMemoryStoragePartition partition = partitions.get(partitionId);

    logger.info("Running put on node: " + nodeId + " partition: " + partitionId);
    partition.put(key, value);

  }

  /**
   * Get a value from storage.
   * @param partitionId - The partition to read from: should map directly to Kafka
   * @param key - The key of the data to be queried
   * @throws VeniceStorageException if the specified partitionId does not exist on this node
   */
  @Override
  public Object get(int partitionId, String key) throws VeniceStorageException {

    if (!partitions.containsKey(partitionId)) {
      throw new VeniceStorageException("On get: PartitionId " + partitionId + " does not exist on nodeId " + nodeId);
    }

    return partitions.get(partitionId).get(key);

  }

  /**
   * Remove a value from storage.
   * @param partitionId - The partition to read from: should map directly to Kafka
   * @param key - The key of the data to be deleted
   * @throws VeniceStorageException if the specified partitionId does not exist on this node
   */
  @Override
  public void delete(int partitionId, String key) throws VeniceStorageException {

    if (!partitions.containsKey(partitionId)) {
      throw new VeniceStorageException("On get: PartitionId " + partitionId + " does not exist on nodeId " + nodeId);
    }

    logger.info("Run a delete on node: " + nodeId + " partition: " + partitionId);
    partitions.get(partitionId).delete(key);

  }

}
