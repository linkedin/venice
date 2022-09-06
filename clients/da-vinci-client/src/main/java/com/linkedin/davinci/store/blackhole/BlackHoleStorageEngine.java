package com.linkedin.davinci.store.blackhole;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import java.util.Collections;
import java.util.Set;


public class BlackHoleStorageEngine extends AbstractStorageEngine<BlackHoleStorageEnginePartition> {
  public BlackHoleStorageEngine(String storeName) {
    super(
        storeName,
        AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer());
    restoreStoragePartitions();
  }

  @Override
  public PersistenceType getType() {
    return PersistenceType.BLACK_HOLE;
  }

  @Override
  protected Set<Integer> getPersistedPartitionIds() {
    return Collections.emptySet();
  }

  @Override
  public BlackHoleStorageEnginePartition createStoragePartition(StoragePartitionConfig storagePartitionConfig) {
    return new BlackHoleStorageEnginePartition(storagePartitionConfig.getPartitionId());
  }

  @Override
  public long getStoreSizeInBytes() {
    return 0;
  }
}
