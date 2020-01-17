package com.linkedin.venice.store.blackhole;

import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.offsets.OffsetRecord;
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
  public void putPartitionOffset(int partitionId, OffsetRecord offsetRecord) {
    throw new RuntimeException("putPartitionOffset not supported in BlackHoleStorageEngine yet!");
  }

  @Override
  public OffsetRecord getPartitionOffset(int partitionId) {
    throw new RuntimeException("getPartitionOffset not supported in BlackHoleStorageEngine yet!");
  }

  @Override
  public long getStoreSizeInBytes() {
    return 0;
  }
}
