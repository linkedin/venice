package com.linkedin.davinci.store;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import java.util.Set;


/**
 * An abstraction that represents the shared resources of a persistence engine.
 * This could include file handles, db connection pools, caches, etc.
 *
 * For example for BDB it holds the various environments, for jdbc it holds a
 * connection pool reference
 */
public abstract class StorageEngineFactory {
  /**
   * Get an initialized storage implementation
   *
   * @param storeDef store definition
   * @return The storage engine
   */
  public abstract StorageEngine getStorageEngine(VeniceStoreVersionConfig storeDef)
      throws StorageInitializationException;

  /**
   * Replication metadata is only supported in RocksDB storage engine. For other type of the storage engine, we will
   * throw VeniceException here.
   */
  public StorageEngine getStorageEngine(VeniceStoreVersionConfig storeDef, boolean replicationMetadataEnabled) {
    return getStorageEngine(storeDef, replicationMetadataEnabled, false);
  }

  public StorageEngine getStorageEngine(
      VeniceStoreVersionConfig storeDef,
      boolean replicationMetadataEnabled,
      boolean mergedValueRmdColumnFamilyEnabled) {
    if (replicationMetadataEnabled || mergedValueRmdColumnFamilyEnabled) {
      throw new VeniceException("Replication metadata is only supported in RocksDB storage engine!");
    }
    return getStorageEngine(storeDef);
  }

  /**
   * Retrieve all the stores persisted previously
   *
   * @return All the store names
   */
  public abstract Set<String> getPersistedStoreNames();

  /**
   * Close the storage configuration
   */
  public abstract void close();

  /**
   * Remove the storage engine from the underlying storage configuration
   *
   * @param engine Specifies the storage engine to be removed
   */
  public abstract void removeStorageEngine(StorageEngine engine);

  /**
   * Remove the storage engine without opening it.
   */
  public abstract void removeStorageEngine(String storeName);

  public abstract void removeStorageEnginePartition(String storeName, int partition);

  /**
   * Close the storage engine from the underlying storage configuration
   *
   * @param engine Specifies the storage engine to be removed
   */
  public abstract void closeStorageEngine(StorageEngine engine);

  /**
   * Return the persistence type current factory supports.
   * @return
   */
  public abstract PersistenceType getPersistenceType();

  public void verifyPersistenceType(VeniceStoreVersionConfig storeConfig) {
    if (!storeConfig.getStorePersistenceType().equals(getPersistenceType())) {
      throw new VeniceException(
          "Required store persistence type: " + storeConfig.getStorePersistenceType() + " of store: "
              + storeConfig.getStoreVersionName() + " isn't supported in current factory: " + getClass().getName()
              + " with type: " + getPersistenceType());
    }
  }

  public void verifyPersistenceType(StorageEngine engine) {
    if (!engine.getType().equals(getPersistenceType())) {
      throw new VeniceException(
          "Required store persistence type: " + engine.getType() + " of store: " + engine.getStoreVersionName()
              + " isn't supported in current factory: " + getClass().getName() + " with type: " + getPersistenceType());
    }
  }
}
