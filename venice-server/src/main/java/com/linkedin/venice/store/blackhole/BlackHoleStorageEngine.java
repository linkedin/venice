package com.linkedin.venice.store.blackhole;

import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.AbstractStoragePartition;
import com.linkedin.venice.store.StoragePartitionConfig;
import java.util.Collections;
import java.util.Set;


public class BlackHoleStorageEngine extends AbstractStorageEngine {
  public BlackHoleStorageEngine(String storeName) {
    super(storeName);
  }

  @Override
  public PersistenceType getType() {
    return PersistenceType.BLACK_HOLE;
  }

  @Override
  protected Set<Integer> getPersistedPartitionIds() {
    return Collections.EMPTY_SET;
  }

  @Override
  public AbstractStoragePartition createStoragePartition(StoragePartitionConfig storagePartitionConfig) {
    return new BlackHoleStorageEnginePartition(storagePartitionConfig.getPartitionId());
  }

  @Override
  public long getStoreSizeInBytes() {
    return 0;
  }
}
