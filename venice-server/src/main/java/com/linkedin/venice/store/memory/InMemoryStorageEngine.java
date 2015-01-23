package com.linkedin.venice.store.memory;

import com.linkedin.venice.server.PartitionNodeAssignmentRepository;
import com.linkedin.venice.server.VeniceConfig;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.AbstractStoragePartition;
import com.linkedin.venice.store.iterators.CloseableStoreEntriesIterator;
import com.linkedin.venice.store.iterators.CloseableStoreKeysIterator;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;


/**
 * A simple non-persistent, in-memory store.
 *
 *
 */
public class InMemoryStorageEngine extends AbstractStorageEngine {
  private static final Logger logger = Logger.getLogger(InMemoryStorageEngine.class);

  public InMemoryStorageEngine(VeniceConfig config, Properties storeDef,
      PartitionNodeAssignmentRepository partitionNodeAssignmentRepo) {
    super(config, storeDef, partitionNodeAssignmentRepo, new ConcurrentHashMap<Integer, AbstractStoragePartition>());

    // Create and intialize the individual databases for each partition
    for (int partitionId : partitionNodeAssignmentRepo
        .getLogicalPartitionIds(this.getName(), this.config.getNodeId())) {
      addStoragePartition(partitionId);
    }
  }

  @Override
  public void addStoragePartition(int partitionId) {
    if (partitionIdToDataBaseMap.containsKey(partitionId)) {
      // TODO log error and throw appropriate exception here
    }
    partitionIdToDataBaseMap.put(partitionId, new InMemoryStoragePartition(partitionId));
  }

  @Override
  public AbstractStoragePartition removePartition(int partitionId) {
    if (!partitionIdToDataBaseMap.containsKey(partitionId)) {
      // TODO log error and throw appropriate exception here
    }
    InMemoryStoragePartition inMemoryStoragePartition =
        (InMemoryStoragePartition) partitionIdToDataBaseMap.remove(partitionId);
    return inMemoryStoragePartition;
  }

  public CloseableStoreEntriesIterator storeEntries() {
    return new CloseableStoreEntriesIterator(partitionIdToDataBaseMap.values());
  }

  public CloseableStoreKeysIterator storeKeys() {
    return new CloseableStoreKeysIterator(storeEntries());
  }

  @Override
  public boolean beginBatchWrites() {
    // Nothing to do here. No batch mode supported in inMemory storage engine
    return false;
  }

  @Override
  public boolean endBatchWrites() {
    // Nothing to do here. No batch mode supported in inMemory storage engine
    return false;
  }
}
