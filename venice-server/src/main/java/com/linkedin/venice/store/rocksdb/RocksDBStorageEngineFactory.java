package com.linkedin.venice.store.rocksdb;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.stats.RocksDBMemoryStats;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StorageEngineFactory;
import java.io.File;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.log4j.Logger;
import org.rocksdb.Cache;
import org.rocksdb.Env;
import org.rocksdb.HistogramType;
import org.rocksdb.LRUCache;
import org.rocksdb.Priority;
import org.rocksdb.RateLimiter;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileManager;
import org.rocksdb.Statistics;
import org.rocksdb.WriteBufferManager;


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

  /**
   * Throttler for RocksDB open operations.
   */
  private final RocksDBThrottler rocksDBThrottler;

  /**
   * Rate limiter for flush and compaction.
   */
  private final RateLimiter rateLimiter;

  private final InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer;
  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;

  public RocksDBStorageEngineFactory(VeniceServerConfig serverConfig) {
    this(serverConfig, null, AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer());
  }
  public RocksDBStorageEngineFactory(VeniceServerConfig serverConfig,
                                     RocksDBMemoryStats rocksDBMemoryStats,
                                     InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer,
                                     InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer) {
    this.rocksDBServerConfig = serverConfig.getRocksDBServerConfig();
    this.rocksDBPath = serverConfig.getDataBasePath() + File.separator + "rocksdb";
    this.rocksDBMemoryStats = rocksDBMemoryStats;
    this.storeVersionStateSerializer = storeVersionStateSerializer;
    this.partitionStateSerializer = partitionStateSerializer;

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
    this.rocksDBThrottler = new RocksDBThrottler(rocksDBServerConfig.getDatabaseOpenOperationThrottle());
    this.rateLimiter = new RateLimiter(rocksDBServerConfig.getWriteQuotaBytesPerSecond());
  }

  public Optional<Statistics> getAggStatistics() {
    return aggStatistics;
  }

  public WriteBufferManager getWriteBufferManager() {
    return writeBufferManager;
  }

  public RateLimiter getRateLimiter() {
    return rateLimiter;
  }

  public SstFileManager getSstFileManager() {
    return sstFileManager;
  }

  public Env getEnv() {
    return env;
  }

  public Cache getSharedCache() {
    return sharedCache;
  }

  @Override
  public synchronized AbstractStorageEngine getStorageEngine(VeniceStoreConfig storeConfig) throws StorageInitializationException {
    verifyPersistenceType(storeConfig);
    final String storeName = storeConfig.getStoreName();
    try {
      if (!storageEngineMap.containsKey(storeName)) {
        storageEngineMap.put(storeName, new RocksDBStorageEngine(storeConfig, this, rocksDBPath, rocksDBMemoryStats,
            rocksDBThrottler, rocksDBServerConfig, storeVersionStateSerializer, partitionStateSerializer));
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
    });
    storageEngineMap.clear();
    sharedCache.close();
    writeBufferManager.close();
    rateLimiter.close();
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
      LOGGER.info("Finished removing RocksDB storage engine for store: " + storeName);
    } else {
      LOGGER.info("RocksDB store: " + storeName + " doesn't exist");
    }
  }

  @Override
  public synchronized void closeStorageEngine(AbstractStorageEngine engine) {
    verifyPersistenceType(engine);
    final String storeName = engine.getName();
    if (storageEngineMap.containsKey(storeName)) {
      LOGGER.info("Started closing RocksDB storage engine for store: " + storeName);
      storageEngineMap.get(storeName).close();
      storageEngineMap.remove(storeName);
      LOGGER.info("Finished closing RocksDB storage engine for store: " + storeName);
    } else {
      LOGGER.info("RocksDB store: " + storeName + " doesn't exist");
    }
  }

  @Override
  public PersistenceType getPersistenceType() {
    return PersistenceType.ROCKS_DB;
  }
}
