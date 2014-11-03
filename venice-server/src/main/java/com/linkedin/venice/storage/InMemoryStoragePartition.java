package com.linkedin.venice.storage;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;


/**
 * An in-memory hashmap implementation of a storage partition
 */
public class InMemoryStoragePartition extends VeniceStoragePartition {

  static final Logger logger = Logger.getLogger(InMemoryStoragePartition.class.getName());

  private Map<String, Object> storage;
  private int store_id = -1;

  /**
   * Constructor
   */
  public InMemoryStoragePartition(int store_id) {

    // initialize empty storage unit
    storage = new HashMap<String, Object>();
    this.store_id = store_id;
  }

  /**
   * Returns the id of this given partition
   */
  public int getId() {
    return store_id;
  }

  /**
   * Puts a value into the key value store
   */
  public void put(String key, Object payload) {
    storage.put(key, payload);
  }

  /**
   * Gets a value from the key value store
   */
  public Object get(String key) {

    if (storage.containsKey(key)) {
      return storage.get(key);
    }

    logger.warn("Cannot find object with key: " + key);
    return null;
  }

  /**
   * Deletes a value from the key value store
   */
  public void delete(String key) {
    storage.remove(key);
  }
}


