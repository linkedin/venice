package com.linkedin.venice.store.memory;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.server.PartitionAssignmentRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StorageEngineFactory;


public class InMemoryStorageEngineFactory implements StorageEngineFactory {
  private static final String TYPE_NAME = "memory";
  private final Object lock = new Object();

  private final PartitionAssignmentRepository partitionNodeAssignmentRepo;

  public InMemoryStorageEngineFactory(VeniceServerConfig serverConfig, PartitionAssignmentRepository partitionNodeAssignmentRepo) {
    this.partitionNodeAssignmentRepo = partitionNodeAssignmentRepo;
  }

  @Override
  public AbstractStorageEngine getStore(VeniceStoreConfig storeDef)
      throws StorageInitializationException {
    synchronized (lock) {
      try {
        return new InMemoryStorageEngine(storeDef, partitionNodeAssignmentRepo);
      } catch (Exception e) {
        throw new StorageInitializationException(e);
      }
    }
  }

  @Override
  public String getType() {
    return TYPE_NAME;
  }

  @Override
  public void update(VeniceStoreConfig storeDef) {
    throw new UnsupportedOperationException(
        "Storage config updates not permitted for " + this.getClass().getCanonicalName());
  }

  @Override
  public void close() {
    //Nothing to do here since we are not tracking specific created environments.
  }

  @Override
  public void removeStorageEngine(AbstractStorageEngine engine) {
    // Nothing to do here since we do not track the created storage engine
  }
}
