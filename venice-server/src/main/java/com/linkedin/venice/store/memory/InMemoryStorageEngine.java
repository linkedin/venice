package com.linkedin.venice.store.memory;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StoragePartitionConfig;

import java.util.Collections;
import java.util.Set;


/**
 * A simple non-persistent, in-memory store.
 */
public class InMemoryStorageEngine extends AbstractStorageEngine<InMemoryStoragePartition> {

  public InMemoryStorageEngine(VeniceStoreConfig storeDef) {
    super(storeDef.getStoreName());
    restoreStoragePartitions();
  }

  @Override
  public PersistenceType getType() {
    return PersistenceType.IN_MEMORY;
  }

  @Override
  protected Set<Integer> getPersistedPartitionIds() {
    // Nothing to return for InMemoryStorageEngine
    return Collections.emptySet();
  }

  @Override
  public InMemoryStoragePartition createStoragePartition(StoragePartitionConfig storagePartitionConfig) {
    return new InMemoryStoragePartition(storagePartitionConfig.getPartitionId());
  }

  @Override
  public long getStoreSizeInBytes() {
    // Not supported
    return StatsErrorCode.NOT_SUPPORTED.code;
  }
}
