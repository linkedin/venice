package com.linkedin.venice.store.blackhole;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StorageEngineFactory;
import java.util.Collections;
import java.util.Set;


public class BlackHoleStorageEngineFactory extends StorageEngineFactory {
  @Override
  public AbstractStorageEngine getStorageEngine(VeniceStoreConfig storeDef) throws StorageInitializationException {
    return new BlackHoleStorageEngine(storeDef.getStoreName());
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
  public void closeStorageEngine(AbstractStorageEngine engine) {
    // Right away!
  }

  @Override
  public PersistenceType getPersistenceType() {
    return PersistenceType.BLACK_HOLE;
  }
}
