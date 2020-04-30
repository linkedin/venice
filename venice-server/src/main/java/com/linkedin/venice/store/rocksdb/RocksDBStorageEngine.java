package com.linkedin.venice.store.rocksdb;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.stats.RocksDBMemoryStats;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StoragePartitionConfig;
import java.io.File;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.rocksdb.Options;


class RocksDBStorageEngine extends AbstractStorageEngine<RocksDBStoragePartition> {
  private static final Logger LOGGER = Logger.getLogger(RocksDBStorageEngine.class);

  private final String rocksDbPath;
  private final String storeDbPath;
  private final Options options;
  private final RocksDBMemoryStats memoryStats;
  private final RocksDBThrottler rocksDbThrottler;


  public RocksDBStorageEngine(VeniceStoreConfig storeConfig, Options options, String rocksDbPath,
      RocksDBMemoryStats rocksDBMemoryStats, RocksDBThrottler rocksDbThrottler) {
    super(storeConfig.getStoreName());
    this.rocksDbPath = rocksDbPath;
    this.options = options;
    this.memoryStats = rocksDBMemoryStats;
    this.rocksDbThrottler = rocksDbThrottler;

    // Create store folder if it doesn't exist
    storeDbPath = RocksDBUtils.composeStoreDbDir(this.rocksDbPath, getName());
    File storeDbDir = new File(storeDbPath);
    if (!storeDbDir.exists()) {
      storeDbDir.mkdirs();
      LOGGER.info("Created RocksDb dir for store: " + getName());
    }

    // Load the existing partitions
    restoreStoragePartitions();
  }

  @Override
  public PersistenceType getType() {
    return PersistenceType.ROCKS_DB;
  }

  @Override
  protected Set<Integer> getPersistedPartitionIds() {
    File storeDbDir = new File(storeDbPath);
    if (!storeDbDir.exists()) {
      LOGGER.info("Store dir: " + storeDbPath + " doesn't exist");
      return new HashSet<>();
    }
    if (!storeDbDir.isDirectory()) {
      throw new VeniceException("Store dir: " + storeDbPath + " is not a directory!!!");
    }
    String[] partitionDbNames = storeDbDir.list();
    HashSet<Integer> partitionIdSet = new HashSet<>();
    for (String partitionDbName : partitionDbNames) {
      partitionIdSet.add(RocksDBUtils.parsePartitionIdFromPartitionDbName(partitionDbName));
    }

    return partitionIdSet;
  }

  @Override
  public RocksDBStoragePartition createStoragePartition(StoragePartitionConfig storagePartitionConfig) {
    return new RocksDBStoragePartition(storagePartitionConfig, options, rocksDbPath, memoryStats, rocksDbThrottler);
  }

  @Override
  public void drop() {
    super.drop();
    // Remove store db dir
    File storeDbDir = new File(storeDbPath);
    if (storeDbDir.exists()) {
      LOGGER.info("Started removing database dir: " + storeDbPath + " for store: " + getName());
      if (!storeDbDir.delete()) {
        LOGGER.warn("Failed to remove dir: " + storeDbDir);
      } else {
        LOGGER.info("Finished removing database dir: " + storeDbPath + " for store: " + getName());
      }
    }
  }

  @Override
  public void close() {
    if (this.isOpen.compareAndSet(true, false)) {
      forEachPartition(RocksDBStoragePartition::close);
    }
  }

  @Override
  public long getStoreSizeInBytes() {
    File storeDbDir = new File(storeDbPath);
    if (storeDbDir.exists()) {
      /**
       * {@link FileUtils#sizeOf(File)} will throw {@link IllegalArgumentException} if the file/dir doesn't exist.
       */
      return FileUtils.sizeOf(storeDbDir);
    } else {
      return 0;
    }
  }
 }