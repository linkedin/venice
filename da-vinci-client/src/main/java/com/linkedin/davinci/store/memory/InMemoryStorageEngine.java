package com.linkedin.davinci.store.memory;

import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.StoragePartitionConfig;

import java.util.Collections;
import java.util.Set;


/**
 * A simple non-persistent, in-memory store.
 */
public class InMemoryStorageEngine extends AbstractStorageEngine<InMemoryStoragePartition> {

  public InMemoryStorageEngine(VeniceStoreConfig storeDef) {
    super(storeDef.getStoreName(), AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(), AvroProtocolDefinition.PARTITION_STATE.getSerializer());
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
