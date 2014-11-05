package com.linkedin.venice.store;

import com.linkedin.venice.storage.StorageType;
import java.util.Properties;


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
  public StorageEngine getStore(Properties storeDef);

  /**
   *
   * @return the type of stores returned by this configuration
   */
  public StorageType getType();

  /**
   * Update the storage configuration at runtime
   *
   * @param storeDef new store definition
   */
  public void update(Properties storeDef);

  /**
   * Close the storage configuration
   */
  public void close();

  /**
   * Remove the storage engine from the underlying storage configuration
   *
   * @param engine Specifies the storage engine to be removed
   */
  public void removeStorageEngine(StorageEngine engine);
}

