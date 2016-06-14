package com.linkedin.venice.store.memory;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.server.PartitionAssignmentRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.AbstractStoragePartition;
import com.linkedin.venice.store.iterators.CloseableStoreEntriesIterator;
import com.linkedin.venice.store.iterators.CloseableStoreKeysIterator;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A simple non-persistent, in-memory store.
 */
public class InMemoryStorageEngine extends AbstractStorageEngine {

  public InMemoryStorageEngine(VeniceStoreConfig storeDef)
    throws Exception {
    super(storeDef.getStoreName());
  }

  @Override
  public AbstractStoragePartition createStoragePartition(int partitionId) {
    return  new InMemoryStoragePartition(partitionId);
  }

  public CloseableStoreEntriesIterator storeEntries() throws PersistenceFailureException {
    return new CloseableStoreEntriesIterator(partitionIdToPartitionMap.values(), this);
  }

  public CloseableStoreKeysIterator storeKeys()
    throws PersistenceFailureException {
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
