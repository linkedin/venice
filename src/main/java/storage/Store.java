package storage;

import java.util.HashMap;
import java.util.Map;

import MetadataManager.KeyCache;
import org.apache.log4j.Logger;

/**
 * Class for managing the storage system and its partitions
 * Created by clfung on 9/10/14.
 */
public class Store {

  static final Logger logger = Logger.getLogger(Store.class.getName());

  private Map<Integer, StorePartition> partitions;

  /**
   * Constructor for the Store
   */
  public Store() {
    partitions = new HashMap<Integer, StorePartition>();
  }

  public void addPartition(int store_id) {

    if (partitions.containsKey(store_id)) {
      logger.error("Attempted to add a partition with an id that already exists: " + store_id);
      return;
    }

    partitions.put(store_id, new StorePartition(store_id));

  }

  /**
   * Add a key-value pair to storage
   */
  public void put(String key, Object value) {

    // use the key to find the store_id
    int partitionId = KeyCache.getPartitionId(key);
    StorePartition partition = partitions.get(partitionId);

    partition.put(key, value);

  }

  public Object get(String key) {

    int store_id = KeyCache.getPartitionId(key);

    if (!partitions.containsKey(store_id)) {
      logger.error("Cannot find partition id: " + store_id);
      return null;
    }

    return partitions.get(store_id).get(key);

  }

}
