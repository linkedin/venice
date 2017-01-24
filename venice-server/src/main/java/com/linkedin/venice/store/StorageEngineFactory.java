package com.linkedin.venice.store;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.StorageInitializationException;

import java.util.Set;


/**
 * An abstraction that represents the shared resources of a persistence engine.
 * This could include file handles, db connection pools, caches, etc.
 *
 * For example for BDB it holds the various environments, for jdbc it holds a
 * connection pool reference
 */
public interface StorageEngineFactory {
  /**
   * Get an initialized storage implementation
   *
   * @param storeDef  store definition
   * @return The storage engine
   */
  AbstractStorageEngine getStore(VeniceStoreConfig storeDef)
      throws StorageInitializationException;

  /**
   *
   * @return the type of stores returned by this configuration
   */
  String getType();

  /**
   * Retrieve all the stores persisted previously
   *
   * @return All the store names
   */
  Set<String> getPersistedStoreNames();

  /**
   * Update the storage configuration at runtime
   *
   * @param storeDef new store definition
   */
  void update(VeniceStoreConfig storeDef);

  /**
   * Close the storage configuration
   */
  void close();

  /**
   * Remove the storage engine from the underlying storage configuration
   *
   * @param engine Specifies the storage engine to be removed
   */
  void removeStorageEngine(AbstractStorageEngine engine);
}

