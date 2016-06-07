package com.linkedin.venice.store.bdb;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.server.PartitionAssignmentRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.AbstractStoragePartition;
import com.linkedin.venice.store.iterators.CloseableStoreEntriesIterator;
import com.linkedin.venice.store.iterators.CloseableStoreKeysIterator;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * BDB-JE Storage Engine
 */
public class BdbStorageEngine extends AbstractStorageEngine {

  private final Environment environment;

  private final AtomicBoolean isOpen;
  private final BdbServerConfig bdbServerConfig;
  private final BdbRuntimeConfig bdbRuntimeConfig;
  //protected final BdbEnvironmentStats bdbEnvironmentStats;
  protected final boolean checkpointerOffForBatchWrites;
  private volatile int numOutstandingBatchWriteJobs = 0;

  public BdbStorageEngine(VeniceStoreConfig storeDef,
                          PartitionAssignmentRepository partitionNodeAssignmentRepo,
                          Environment environment ) throws VeniceException {
    super(storeDef, partitionNodeAssignmentRepo, new ConcurrentHashMap<Integer, AbstractStoragePartition>());

    this.environment = environment;
    this.isOpen = new AtomicBoolean(true);
    this.bdbServerConfig = storeDef.getBdbServerConfig();
    this.bdbRuntimeConfig = new BdbRuntimeConfig(bdbServerConfig);
    this.checkpointerOffForBatchWrites = bdbRuntimeConfig.isCheckpointerOffForBatchWrites();
    initialStoreForPartitions(partitionNodeAssignmentRepo);
  }

  @Override
  public synchronized void addStoragePartition(int partitionId) {
    /**
     * If this method is called by anyone other than the constructor, i.e- the admin service, the caller should ensure
     * that after the addition of the storage partition:
     *  1. populate the partition node assignment repository
     *  2. it should also be registered with an SimpleKafkaConsumerTask thread.
     */
    if (partitionIdToPartitionMap.containsKey(partitionId)) {
      logger.error("Failed to add a storage partition for partitionId: " + partitionId + " Store " + this.getName() +" . This partition already exists!");
      throw new StorageInitializationException("Partition " + partitionId + " of store " + this.getName() + " already exists.");
    }
    BdbStoragePartition partition = new BdbStoragePartition(this.getName(), partitionId, environment, bdbServerConfig);
    partitionIdToPartitionMap.put(partitionId, partition);
  }

  @Override
  public synchronized void dropPartition(int partitionId) {
    /**
     * The caller of this method should ensure that:
     * 1. The SimpleKafkaConsumerTask associated with this partition is shutdown
     * 2. The partition node assignment repo is cleaned up and then remove this storage partition.
     *    Else there can be situations where the data is consumed from Kafka and not persisted.
     */
    if (!partitionIdToPartitionMap.containsKey(partitionId)) {
      logger.error("Failed to remove a non existing partition: " + partitionId + " Store " + this.getName() );
      throw new VeniceException("Partition " + partitionId + " of store " + this.getName() + " does not exist.");
    }
    /* NOTE: bdb database is not closed here. */
    logger.info("Removing Partition: " + partitionId + " Store " + this.getName() );
    BdbStoragePartition partition = (BdbStoragePartition) partitionIdToPartitionMap.remove(partitionId);
    partition.drop();
    if(partitionIdToPartitionMap.size() == 0) {
      logger.info("All Partitions deleted for Store " + this.getName() );
    }
  }

  public CloseableStoreEntriesIterator storeEntries() {
    return new CloseableStoreEntriesIterator(partitionIdToPartitionMap.values(), this);
  }

  public CloseableStoreKeysIterator storeKeys() {
    return new CloseableStoreKeysIterator(storeEntries());
  }

  @Override
  public boolean beginBatchWrites() {
    if (checkpointerOffForBatchWrites) {
      synchronized (this) {
        numOutstandingBatchWriteJobs++;
        // turn the checkpointer off for the first job
        if (numOutstandingBatchWriteJobs == 1) {
          logger.info("Turning checkpointer off for batch writes");
          EnvironmentMutableConfig mConfig = environment.getMutableConfig();
          mConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
            Boolean.toString(false));
          environment.setMutableConfig(mConfig);
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean endBatchWrites() {
    if (checkpointerOffForBatchWrites) {
      synchronized (this) {
        numOutstandingBatchWriteJobs--;
        // turn the checkpointer back on if the last job finishes
        if (numOutstandingBatchWriteJobs == 0) {
          logger.info("Turning checkpointer on");
          EnvironmentMutableConfig mConfig = environment.getMutableConfig();
          mConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
            Boolean.toString(true));
          environment.setMutableConfig(mConfig);
          return true;
        }
      }
    }
    return false;
  }

  public void close() {
    if (this.isOpen.compareAndSet(true, false)) {
      for (AbstractStoragePartition partition : partitionIdToPartitionMap.values()) {
        partition.close();
      }
    }
  }
}
