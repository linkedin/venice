package com.linkedin.venice.config;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.store.bdb.BdbStorageEngineFactory;
import com.linkedin.venice.store.bdb.BdbStoreConfig;
import com.linkedin.venice.store.memory.InMemoryStorageEngineFactory;
import com.linkedin.venice.utils.Props;

import java.util.List;
import java.util.Map;


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
    storeName = storeProperties.getString(VeniceConfigService.STORE_NAME);

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

  public BdbStoreConfig getBdbStoreConfig() {
    return this.bdbStoreConfig;
  }
}
