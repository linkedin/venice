package com.linkedin.venice.store.memory;

import com.linkedin.venice.server.PartitionNodeAssignmentRepository;
import com.linkedin.venice.server.VeniceConfig;
import com.linkedin.venice.storage.StorageType;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StorageEngineFactory;
import java.util.Properties;
import org.apache.log4j.Logger;


public class InMemoryStorageEngineFactory implements StorageEngineFactory {

  private static final Logger logger = Logger.getLogger(InMemoryStorageEngineFactory.class);
  private static final StorageType TYPE_NAME = StorageType.MEMORY;
  private final Object lock = new Object();

  private final VeniceConfig veniceConfig;
  private final PartitionNodeAssignmentRepository partitionNodeAssignmentRepo;

  public InMemoryStorageEngineFactory(VeniceConfig veniceConfig,
      PartitionNodeAssignmentRepository partitionNodeAssignmentRepo) {
    this.veniceConfig = veniceConfig;
    this.partitionNodeAssignmentRepo = partitionNodeAssignmentRepo;
  }

  @Override
  public StorageType getType() {
    return TYPE_NAME;
  }

  @Override
  public AbstractStorageEngine getStore(Properties storeDef) {
    synchronized (lock) {
      return new InMemoryStorageEngine(veniceConfig, storeDef, partitionNodeAssignmentRepo);
    }
  }

  @Override
  public void update(Properties storeDef) {
    //TODO use appropriate exception to track
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
