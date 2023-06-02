package com.linkedin.davinci.store.blackhole;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.StorageEngineFactory;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.meta.PersistenceType;
import java.util.Collections;
import java.util.Set;


public class BlackHoleStorageEngineFactory extends StorageEngineFactory {
  @Override
  public AbstractStorageEngine getStorageEngine(VeniceStoreVersionConfig storeDef)
      throws StorageInitializationException {
    return new BlackHoleStorageEngine(storeDef.getStoreVersionName());
  }

  @Override
  public Set<String> getPersistedStoreNames() {
    return Collections.EMPTY_SET;
  }

  @Override
  public void close() {
    // kbye
  }

  @Override
  public void removeStorageEngine(AbstractStorageEngine engine) {
    // Right away!
  }

  @Override
  public void removeStorageEngine(String storeName) {
    // Right away!
  }

  @Override
  public void closeStorageEngine(AbstractStorageEngine engine) {
    // Right away!
  }

  @Override
  public PersistenceType getPersistenceType() {
    return PersistenceType.BLACK_HOLE;
  }
}
