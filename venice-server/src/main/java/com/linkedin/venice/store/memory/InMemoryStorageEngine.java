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

  public InMemoryStorageEngine(VeniceConfig config, Properties storeDef,
      PartitionNodeAssignmentRepository partitionNodeAssignmentRepo)
      throws Exception {
    super(config, storeDef, partitionNodeAssignmentRepo, new ConcurrentHashMap<Integer, AbstractStoragePartition>());

    // Create and intialize the individual databases for each partition
    for (int partitionId : partitionNodeAssignmentRepo
        .getLogicalPartitionIds(this.getName(), this.config.getNodeId())) {
      addStoragePartition(partitionId);
    }
  }

  @Override
  public void addStoragePartition(int partitionId)
      throws Exception {
    /**
     * If this method is called by anyone other than the constructor, i.e- the admin service, the caller should ensure
     * that after the addition of the storage partition:
     *  1. populate the partitio node assignment repository
     *  2. it should also be registered with an SimpleKafkaConsumerTask
     *     thread.
     */
    if (partitionIdToDataBaseMap.containsKey(partitionId)) {
      String errorMessage =
          "Failed to add a storage partition for partitionId: " + partitionId + " . This partition already exists!";
      logger.error(errorMessage);
      throw new Exception(errorMessage); // TODO Later change this to appropriate Exception type
    }
    partitionIdToDataBaseMap.put(partitionId, new InMemoryStoragePartition(partitionId));
  }

  @Override
  public AbstractStoragePartition removePartition(int partitionId)
      throws Exception {
    /**
     * The caller of this method should ensure that 1. first the SimpleKafkaConsumerTask associated with this partition is
     * shutdown 2. parittion node Assignment repo is cleaned up and 3. then remove this storage partition. Else there can 
     * be situations where the data is consumed from Kafka and not persisted.
     */
    if (!partitionIdToDataBaseMap.containsKey(partitionId)) {
      String errorMessage = "Failed to remove a non existing partition: " + partitionId;
      logger.error(errorMessage);
      throw new Exception(errorMessage); // TODO Later change this to appropriate Exception type
    }
    InMemoryStoragePartition inMemoryStoragePartition =
        (InMemoryStoragePartition) partitionIdToDataBaseMap.remove(partitionId);
    return inMemoryStoragePartition;
  }

  public CloseableStoreEntriesIterator storeEntries()
      throws Exception { // TODO Change it to Appropriate Exception later
    return new CloseableStoreEntriesIterator(partitionIdToDataBaseMap.values());
  }

  public CloseableStoreKeysIterator storeKeys()
      throws Exception { // TODO Change it to Appropriate Exception later
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
