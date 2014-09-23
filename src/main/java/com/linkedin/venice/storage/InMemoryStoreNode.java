package com.linkedin.venice.storage;

import com.linkedin.venice.metadata.KeyCache;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by clfung on 9/17/14.
 */
public class InMemoryStoreNode extends VeniceStoreNode {

  static final Logger logger = Logger.getLogger(InMemoryStoreNode.class.getName());

  private int nodeId = -1;

  // Map which stores the partitions and their associated ids
  private static Map<Integer, InMemoryStorePartition> partitions = new HashMap<Integer, InMemoryStorePartition>();
  private static KeyCache keyCache;

  // number of partitions in the store
  private static int partitionCount = -1;

  public InMemoryStoreNode(int nodeId) {

    // register current nodeId
    this.nodeId = nodeId;

    // create static instance of keyCache
    keyCache = KeyCache.getInstance();

    // create single partition for Node
    partitions.put(1, new InMemoryStorePartition(1));
    partitionCount = 1;


  }

  public int getNodeId() {
    return nodeId;
  }

  /**
   * Initializes new partitions in a storage instance
   * @param newPartitions - number of partitions to initialize
   * */
  public void addPartitions(int newPartitions) {

    for (int i = 0; i < newPartitions; i++) {
      addPartition();
    }

  }

  /**
   * Adds a partition to the current Store, using autoincrement ids
   * Values start at 0
   *
   */
  protected synchronized void addPartition() {

    partitionCount++;
    partitions.put(partitionCount, new InMemoryStorePartition(partitionCount));

  }

  /**
   * Adds a partition to the current Store
   * TODO: I'm not sure if this is required. Depends if auto-increment or hashing will be used for ids
   * @param store_id - id of partition to add
   */
  protected void addPartition(int store_id) {

    if (partitions.containsKey(store_id)) {
      logger.error("Attempted to add a partition with an id that already exists: " + store_id);
      return;
    }

    partitions.put(store_id, new InMemoryStorePartition(store_id));

  }

  /**
   * Add a key-value pair to storage. Partitioning is handled internally.
   * @param key - The key of the data in the KV pair
   * @param value - The value of the data in the KV pair
   */
  public void put(String key, Object value) {

    // use the key to find the store_id
    int partitionId = keyCache.getKeyAddress(key).getPartitionId();
    InMemoryStorePartition partition = partitions.get(partitionId);

    logger.info("Run a put on node: " + nodeId + " partition: " + partitionId);
    partition.put(key, value);

  }

  /**
   * Get a value from storage. Partitioning is handled internally.
   * @param key - The key of the data to be queried
   */
  public Object get(String key) {

    int partitionId = keyCache.getKeyAddress(key).getPartitionId();

    if (!partitions.containsKey(partitionId)) {
      logger.error("Cannot find partition id: " + partitionId);
      return null;
    }

    return partitions.get(partitionId).get(key);

  }

}
