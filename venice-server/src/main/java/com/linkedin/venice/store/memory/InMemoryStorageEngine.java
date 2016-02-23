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

import java.util.concurrent.ConcurrentHashMap;


/**
 * A simple non-persistent, in-memory store.
 */
public class InMemoryStorageEngine extends AbstractStorageEngine {

  public InMemoryStorageEngine(VeniceStoreConfig storeDef,
                               PartitionAssignmentRepository partitionNodeAssignmentRepo)
    throws Exception {
    super(storeDef, partitionNodeAssignmentRepo, new ConcurrentHashMap<Integer, AbstractStoragePartition>());

    initialStoreForPartitions(partitionNodeAssignmentRepo);
  }

  @Override
  public void addStoragePartition(int partitionId)
    throws VeniceException {
    /**
     * If this method is called by anyone other than the constructor, i.e- the admin service, the caller should ensure
     * that after the addition of the storage partition:
     *  1. populate the partition node assignment repository
     *  2. it should also be registered with an SimpleKafkaConsumerTask
     *     thread.
     */
    if (partitionIdToPartitionMap.containsKey(partitionId)) {
      String errorMessage =
        "Failed to add a storage partition for partitionId: " + partitionId + " . This partition already exists!";
      logger.error(errorMessage);
      throw new StorageInitializationException(errorMessage);
    }
    partitionIdToPartitionMap.put(partitionId, new InMemoryStoragePartition(partitionId));
  }

  @Override
  public AbstractStoragePartition removePartition(int partitionId)
    throws VeniceException {
    /**
     * The caller of this method should ensure that 1. first the SimpleKafkaConsumerTask associated with this partition is
     * shutdown 2. parittion node Assignment repo is cleaned up and 3. then remove this storage partition. Else there can 
     * be situations where the data is consumed from Kafka and not persisted.
     */
    if (!partitionIdToPartitionMap.containsKey(partitionId)) {
      String errorMessage = "Failed to remove a non existing partition: " + partitionId;
      logger.error(errorMessage);
      throw new VeniceException(errorMessage); // TODO Later change this to appropriate Exception type
    }
    InMemoryStoragePartition inMemoryStoragePartition =
      (InMemoryStoragePartition) partitionIdToPartitionMap.remove(partitionId);
    return inMemoryStoragePartition;
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
