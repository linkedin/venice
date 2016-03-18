package com.linkedin.venice.config;

import static com.linkedin.venice.ConfigKeys.*;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.store.bdb.BdbStoreConfig;
import com.linkedin.venice.utils.Props;

/**
 * class that maintains all properties that are not specific to a venice server and cluster.
 * Includes individual store properties and other properties that can be overwritten.
 */
public class VeniceStoreConfig extends VeniceServerConfig {

  private String storeName;

  // TODO: Store level bdb configuration, need to create StoreStorageConfig abstract class and extend from that
  private BdbStoreConfig bdbStoreConfig;

  public VeniceStoreConfig(Props storeProperties)
    throws ConfigurationException {
    super(storeProperties);
    initAndValidateProperties(storeProperties);
  }

  private void initAndValidateProperties(Props storeProperties) throws ConfigurationException {
    storeName = storeProperties.getString(STORE_NAME);

    if (getPersistenceType().equals(PersistenceType.BDB)) {
      bdbStoreConfig = new BdbStoreConfig(storeName, storeProperties);
    } else {
      bdbStoreConfig = null;
    }
    // initialize all other properties here and add getters for the same.
  }

  public String getStoreName() {
    return storeName;
  }

  public String getStorageEngineFactoryClassName() {
    return storageEngineFactoryClassNameMap.get(this.getPersistenceType());
  }

  // TODO: This function doesn't belong here, does it ?!?!?
  public BdbStoreConfig getBdbStoreConfig() {
    if (getPersistenceType().equals(PersistenceType.BDB)) {
      return this.bdbStoreConfig;
    } else {
      throw new VeniceException("Store '" + storeName + "' is not BDB, so it does not have any BdbStoreConfig.");
    }
  }
}
