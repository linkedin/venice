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
  protected final boolean checkpointerOffForBatchWrites;
  private volatile int numOutstandingBatchWriteJobs = 0;

  public BdbStorageEngine(VeniceStoreConfig storeDef,
                          Environment environment ) throws VeniceException {
    super(storeDef.getStoreName());

    this.environment = environment;
    this.isOpen = new AtomicBoolean(true);
    this.bdbServerConfig = storeDef.getBdbServerConfig();
    this.bdbRuntimeConfig = new BdbRuntimeConfig(bdbServerConfig);
    this.checkpointerOffForBatchWrites = bdbRuntimeConfig.isCheckpointerOffForBatchWrites();
  }

  @Override
  public AbstractStoragePartition createStoragePartition(int partitionId) {
    return new BdbStoragePartition(this.getName(), partitionId, environment, bdbServerConfig);
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
