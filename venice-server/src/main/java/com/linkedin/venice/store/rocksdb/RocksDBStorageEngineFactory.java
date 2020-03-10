package com.linkedin.venice.store.rocksdb;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.stats.RocksDBMemoryStats;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StorageEngineFactory;

import org.apache.log4j.Logger;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.Env;
import org.rocksdb.HistogramType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.Priority;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileManager;
import org.rocksdb.Statistics;
import org.rocksdb.WriteBufferManager;

import java.io.File;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


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
  private final Optional<Statistics> aggStatistics;

  /**
   * https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager
   * Setup write buffer manager to limit the total memtable usages to block cache
   */
  private final WriteBufferManager writeBufferManager;

  /**
   * A default SstFileManager object is created, which creates a DeleteScheduler object which in turn
   * creates a background thread to handle file deletion.
   * We would like to share the same SstFileManager across all the databases.
   */
  private final SstFileManager sstFileManager;

  private final RocksDBMemoryStats rocksDBMemoryStats;

  public RocksDBStorageEngineFactory(VeniceServerConfig serverConfig) {
    this(serverConfig, null);
  }
  public RocksDBStorageEngineFactory(VeniceServerConfig serverConfig, RocksDBMemoryStats rocksDBMemoryStats) {
    this.rocksDBServerConfig = serverConfig.getRocksDBServerConfig();
    this.rocksDBPath = serverConfig.getDataBasePath() + File.separator + "rocksdb";
    this.rocksDBMemoryStats = rocksDBMemoryStats;

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

    if (rocksDBServerConfig.isRocksDBStatisticsEnabled()) {
      // Ignore all the histogram types for performance concern.
      this.aggStatistics = Optional.of(new Statistics(EnumSet.allOf(HistogramType.class)));
    } else {
      this.aggStatistics = Optional.empty();
    }

    // Write buffer manager across all the RocksDB databases
    // The memory usage of all the memtables will cost to the shared block cache
    this.writeBufferManager = new WriteBufferManager(rocksDBServerConfig.getRocksDBTotalMemtableUsageCapInBytes(), this.sharedCache);
    try {
      this.sstFileManager = new SstFileManager(this.env);
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to create the shared SstFileManager", e);
    }
  }

  public Optional<Statistics> getAggStatistics() {
    return aggStatistics;
  }

  private synchronized Options getStoreOptions(String storeName) {
    if (storageEngineOptions.containsKey(storeName)) {
      return storageEngineOptions.get(storeName);
    }

    Options options = new Options();
    options.setEnv(env);
    options.setCreateIfMissing(true);
    options.setCompressionType(rocksDBServerConfig.getRocksDBOptionsCompressionType());
    options.setCompactionStyle(rocksDBServerConfig.getRocksDBOptionsCompactionStyle());
    options.setBytesPerSync(rocksDBServerConfig.getRocksDBBytesPerSync());
    options.setUseDirectReads(rocksDBServerConfig.getRocksDBUseDirectReads());
    options.setMaxOpenFiles(rocksDBServerConfig.getMaxOpenFiles());
    options.setTargetFileSizeBase(rocksDBServerConfig.getTargetFileSizeInBytes());

    options.setWriteBufferManager(writeBufferManager);
    options.setSstFileManager(sstFileManager);
    /**
     * Disable the stat dump threads, which will create excessive threads, which will eventually crash
     * storage node.
     */
    options.setStatsDumpPeriodSec(0);
    options.setStatsPersistPeriodSec(0);

    aggStatistics.ifPresent(stat -> options.setStatistics(stat));

    if (rocksDBServerConfig.isRocksDBPlainTableFormatEnabled()) {
      PlainTableConfig tableConfig = new PlainTableConfig();
      tableConfig.setStoreIndexInFile(rocksDBServerConfig.isRocksDBStoreIndexInFile());
      tableConfig.setHugePageTlbSize(rocksDBServerConfig.getRocksDBHugePageTlbSize());
      tableConfig.setBloomBitsPerKey(rocksDBServerConfig.getRocksDBBloomBitsPerKey());
      options.setTableFormatConfig(tableConfig);
      options.setAllowMmapReads(true);
    } else {
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
      options.setTableFormatConfig(tableConfig);
    }

    // Memtable options
    options.setWriteBufferSize(rocksDBServerConfig.getRocksDBMemtableSizeInBytes());
    options.setMaxWriteBufferNumber(rocksDBServerConfig.getRocksDBMaxMemtableCount());
    options.setMaxTotalWalSize(rocksDBServerConfig.getRocksDBMaxTotalWalSizeInBytes());
    options.setMaxBytesForLevelBase(rocksDBServerConfig.getRocksDBMaxBytesForLevelBase());

    storageEngineOptions.put(storeName, options);
    return options;
  }

  @Override
  public synchronized AbstractStorageEngine getStore(VeniceStoreConfig storeConfig) throws StorageInitializationException {
    verifyPersistenceType(storeConfig);
    final String storeName = storeConfig.getStoreName();
    try {
      if (!storageEngineMap.containsKey(storeName)) {
        Options storeOptions = getStoreOptions(storeName);
        storageEngineMap.put(storeName, new RocksDBStorageEngine(storeConfig, storeOptions, rocksDBPath, rocksDBMemoryStats));
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
      getStoreOptions(storeName).close();
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
      getStoreOptions(storeName).close();
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
