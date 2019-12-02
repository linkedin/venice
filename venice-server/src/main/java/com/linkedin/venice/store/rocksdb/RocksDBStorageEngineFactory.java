package com.linkedin.venice.store.rocksdb;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StorageEngineFactory;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.Env;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.Priority;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBufferManager;

import static org.rocksdb.Env.*;


public class RocksDBStorageEngineFactory extends StorageEngineFactory {
  /**
   * Need to preload RocksDB libraries.
   */
  static {
    RocksDB.loadLibrary();
  }

  private static final Logger LOGGER = Logger.getLogger(RocksDBStorageEngineFactory.class);

  private final RocksDBServerConfig rocksDBServerConfig;

  // Shared Env across all the RocksDB databases
  private final Env env;

  /**
   * RocksDB root path
   */
  private final String rocksDBPath;

  private final Cache sharedCache;
  private final Map<String, RocksDBStorageEngine> storageEngineMap = new HashMap<>();
  private final Map<String, Options> storageEngineOptions = new HashMap<>();

  /**
   * https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager
   * Setup write buffer manager to limit the total memtable usages to block cache
   */
  private final WriteBufferManager writeBufferManager;

  public RocksDBStorageEngineFactory(VeniceServerConfig serverConfig) {
    this.rocksDBServerConfig = serverConfig.getRocksDBServerConfig();
    this.rocksDBPath = serverConfig.getDataBasePath() + File.separator + "rocksdb";

    /**
     * Shared {@link Env} allows us to share the flush thread pool and compaction thread pool.
     */
    this.env = Env.getDefault();
    /**
     * https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
     * Flush threads are in the HIGH priority pool, while compaction threads are in the LOW priority pool
      */
    this.env.setBackgroundThreads(rocksDBServerConfig.getRocksDBEnvFlushPoolSize(), Priority.HIGH);
    this.env.setBackgroundThreads(rocksDBServerConfig.getRocksDBEnvCompactionPoolSize(), Priority.LOW);

    // Shared cache across all the RocksDB databases
    this.sharedCache = new LRUCache(rocksDBServerConfig.getRocksDBBlockCacheSizeInBytes(),
                               rocksDBServerConfig.getRocksDBBlockCacheShardBits(),
                               rocksDBServerConfig.getRocksDBBlockCacheStrictCapacityLimit());

    // Write buffer manager across all the RocksDB databases
    // The memory usage of all the memtables will cost to the shared block cache
    this.writeBufferManager = new WriteBufferManager(rocksDBServerConfig.getRocksDBTotalMemtableUsageCapInBytes(), this.sharedCache);
  }

  private synchronized Options getOptionsForStore(String storeName) {
    if (storageEngineOptions.containsKey(storeName)) {
      return storageEngineOptions.get(storeName);
    }
    Options newOptions = new Options();
    newOptions.setEnv(this.env);
    newOptions.setCreateIfMissing(true);
    newOptions.setCompressionType(rocksDBServerConfig.getRocksDBOptionsCompressionType());
    newOptions.setCompactionStyle(rocksDBServerConfig.getRocksDBOptionsCompactionStyle());
    newOptions.setBytesPerSync(rocksDBServerConfig.getRocksDBBytesPerSync());

    // Inherit Direct IO for read settings from globals
    newOptions.setUseDirectReads(rocksDBServerConfig.getRocksDBUseDirectReads());

    newOptions.setWriteBufferManager(writeBufferManager);

    // Cache index and bloom filter in block cache
    // and share the same cache across all the RocksDB databases
    BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
    tableConfig.setBlockSize(rocksDBServerConfig.getRocksDBSSTFileBlockSizeInBytes());
    tableConfig.setBlockCache(sharedCache);
    tableConfig.setCacheIndexAndFilterBlocks(true);

    // TODO Consider Adding "cache_index_and_filter_blocks_with_high_priority" to allow for preservation of indexes in memory.
    // https://github.com/facebook/rocksdb/wiki/Block-Cache#caching-index-and-filter-blocks
    // https://github.com/facebook/rocksdb/wiki/Block-Cache#lru-cache

    tableConfig.setBlockCacheCompressedSize(rocksDBServerConfig.getRocksDBBlockCacheCompressedSizeInBytes());
    tableConfig.setFormatVersion(2); // Latest version

    newOptions.setTableFormatConfig(tableConfig);

    // Memtable options
    newOptions.setWriteBufferSize(rocksDBServerConfig.getRocksDBMemtableSizeInBytes());
    newOptions.setMaxWriteBufferNumber(rocksDBServerConfig.getRocksDBMaxMemtableCount());
    newOptions.setMaxTotalWalSize(rocksDBServerConfig.getRocksDBMaxTotalWalSizeInBytes());
    newOptions.setMaxBytesForLevelBase(rocksDBServerConfig.getRocksDBMaxBytesForLevelBase());

    storageEngineOptions.put(storeName, newOptions);

    return newOptions;
  }

  @Override
  public synchronized AbstractStorageEngine getStore(VeniceStoreConfig storeConfig) throws StorageInitializationException {
    verifyPersistenceType(storeConfig);
    final String storeName = storeConfig.getStoreName();
    try {
      if (!storageEngineMap.containsKey(storeName)) {
        Options storeOptions = getOptionsForStore(storeName);
        storageEngineMap.put(storeName, new RocksDBStorageEngine(storeConfig, storeOptions, rocksDBPath));
      }
      return storageEngineMap.get(storeName);
    } catch (Exception e) {
      throw new StorageInitializationException(e);
    }
  }

  @Override
  public synchronized Set<String> getPersistedStoreNames() {
    Set<String> storeNames = new HashSet<>();
    File databaseDir = new File(rocksDBPath);
    if (databaseDir.exists() && databaseDir.isDirectory()) {
      String[] storeDirs = databaseDir.list();
      LOGGER.info("Found the following RocksDB databases: " + Arrays.toString(storeDirs));
      for (String storeName : storeDirs) {
        storeNames.add(storeName);
      }
    } else {
      LOGGER.info("RocksDB master dir: " + databaseDir + " doesn't exist, so nothing to restore");
    }
    return storeNames;
  }

  @Override
  public synchronized void close() {
    LOGGER.info("Closing RocksDBStorageEngineFactory");
    storageEngineMap.forEach( (storeName, storageEngine) -> {
      storageEngine.close();
      getOptionsForStore(storeName).close();
    });
    storageEngineMap.clear();
    storageEngineOptions.clear();
    sharedCache.close();
    writeBufferManager.close();
    this.env.close();
    LOGGER.info("Closed RocksDBStorageEngineFactory");
  }

  @Override
  public synchronized void removeStorageEngine(AbstractStorageEngine engine) {
    verifyPersistenceType(engine);
    final String storeName = engine.getName();
    if (storageEngineMap.containsKey(storeName)) {
      LOGGER.info("Started removing RocksDB storage engine for store: " + storeName);
      storageEngineMap.get(storeName).drop();
      storageEngineMap.remove(storeName);
      getOptionsForStore(storeName).close();
      storageEngineOptions.remove(storeName);
      LOGGER.info("Finished removing RocksDB storage engine for store: " + storeName);
    } else {
      LOGGER.info("RocksDB store: " + storeName + " doesn't exist");
    }
  }

  @Override
  public PersistenceType getPersistenceType() {
    return PersistenceType.ROCKS_DB;
  }
}