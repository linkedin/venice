package com.linkedin.venice.store.bdb;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StoragePartitionConfig;
import com.sleepycat.je.Environment;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;

import static com.linkedin.venice.store.bdb.BdbStoragePartition.*;


/**
 * BDB-JE Storage Engine
 */
public class BdbStorageEngine extends AbstractStorageEngine<BdbStoragePartition> {

  private final Environment environment;

  private final AtomicBoolean isOpen;
  private final BdbServerConfig bdbServerConfig;
  private final BdbRuntimeConfig bdbRuntimeConfig;
  protected final boolean checkpointerOffForBatchWrites;
  private volatile int numOutstandingBatchWriteJobs = 0;

  public BdbStorageEngine(VeniceStoreConfig storeDef,
                          Environment environment) throws VeniceException {
    super(storeDef.getStoreName());

    this.environment = environment;
    this.isOpen = new AtomicBoolean(true);
    this.bdbServerConfig = storeDef.getBdbServerConfig();
    this.bdbRuntimeConfig = new BdbRuntimeConfig(bdbServerConfig);
    this.checkpointerOffForBatchWrites = bdbRuntimeConfig.isCheckpointerOffForBatchWrites();
    // Load the existing partitions
    restoreStoragePartitions();
  }

  public Environment getBdbEnvironment() {
    return this.environment;
  }

  @Override
  public PersistenceType getType() {
    return PersistenceType.BDB;
  }

  @Override
  protected Set<Integer> getPersistedPartitionIds() {
    List<String> partitionNames = environment.getDatabaseNames().stream()
        .filter(partitionName -> !partitionName.endsWith(DELETE_FLAG_SUFFIX)).collect(Collectors.toList());
    Set<Integer> partitionIds = new HashSet<>();
    for (String partitionName : partitionNames) {
      // Make sure to extract partition id from the partitions belonging to current store
      if (BdbStoragePartition.getStoreNameFromPartitionName(partitionName).equals(getName())) {
        partitionIds.add(BdbStoragePartition.getPartitionIdFromPartitionName(partitionName));
      }
    }
    return partitionIds;
  }

  @Override
  public BdbStoragePartition createStoragePartition(StoragePartitionConfig storagePartitionConfig) {
    return new BdbStoragePartition(storagePartitionConfig, environment, bdbServerConfig);
  }

  public void close() {
    if (this.isOpen.compareAndSet(true, false)) {
      forEachPartition(BdbStoragePartition::close);
    }
  }

  @Override
  public long getStoreSizeInBytes() {
    if (environment.isValid()) {
      return FileUtils.sizeOf(environment.getHome());
    } else {
      // Environment could be closed/discarded
      return StatsErrorCode.NULL_BDB_ENVIRONMENT.code;
    }
  }
}
