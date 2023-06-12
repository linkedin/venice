package com.linkedin.davinci.store.rocksdb;

import static org.rocksdb.RateLimiter.DEFAULT_FAIRNESS;
import static org.rocksdb.RateLimiter.DEFAULT_MODE;
import static org.rocksdb.RateLimiter.DEFAULT_REFILL_PERIOD_MICROS;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.StorageEngineFactory;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Cache;
import org.rocksdb.ClockCache;
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

  private static final Logger LOGGER = LogManager.getLogger(RocksDBStorageEngineFactory.class);

  private final RocksDBServerConfig rocksDBServerConfig;

  // Shared Env across all the RocksDB databases
  private final Env env;

  /**
   * RocksDB root path
   */
  private final String rocksDBPath;
  private final Cache sharedCache;
  private Cache sharedRMDCache;
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

  private final long memoryLimit;

  private final long memtableSize;

  public RocksDBStorageEngineFactory(VeniceServerConfig serverConfig) {
    this(
        serverConfig,
        null,
        AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer());
  }

  public RocksDBStorageEngineFactory(
      VeniceServerConfig serverConfig,
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
    if (RocksDBBlockCacheImplementations.CLOCK.equals(rocksDBServerConfig.getRocksDBBlockCacheImplementation())) {
      if (rocksDBServerConfig.isUseSeparateRMDCacheEnabled()) {
        this.sharedRMDCache = new ClockCache(
            rocksDBServerConfig.getRocksDBRMDBlockCacheSizeInBytes(),
            rocksDBServerConfig.getRocksDBBlockCacheShardBits(),
            rocksDBServerConfig.getRocksDBBlockCacheStrictCapacityLimit());
      }
      this.sharedCache = new ClockCache(
          rocksDBServerConfig.getRocksDBBlockCacheSizeInBytes(),
          rocksDBServerConfig.getRocksDBBlockCacheShardBits(),
          rocksDBServerConfig.getRocksDBBlockCacheStrictCapacityLimit());

    } else {
      // Default to LRUCache
      this.sharedCache = new LRUCache(
          rocksDBServerConfig.getRocksDBBlockCacheSizeInBytes(),
          rocksDBServerConfig.getRocksDBBlockCacheShardBits(),
          rocksDBServerConfig.getRocksDBBlockCacheStrictCapacityLimit());
      if (rocksDBServerConfig.isUseSeparateRMDCacheEnabled()) {
        this.sharedRMDCache = new LRUCache(
            rocksDBServerConfig.getRocksDBRMDBlockCacheSizeInBytes(),
            rocksDBServerConfig.getRocksDBBlockCacheShardBits(),
            rocksDBServerConfig.getRocksDBBlockCacheStrictCapacityLimit());
      }
    }

    if (rocksDBServerConfig.isRocksDBStatisticsEnabled()) {
      // Ignore all the histogram types for performance concern.
      this.aggStatistics = Optional.of(new Statistics(EnumSet.allOf(HistogramType.class)));
    } else {
      this.aggStatistics = Optional.empty();
    }

    // Write buffer manager across all the RocksDB databases
    // The memory usage of all the memtables will cost to the shared block cache
    this.writeBufferManager =
        new WriteBufferManager(rocksDBServerConfig.getRocksDBTotalMemtableUsageCapInBytes(), this.sharedCache);
    this.memoryLimit = serverConfig.getIngestionMemoryLimit();
    this.memtableSize = rocksDBServerConfig.getRocksDBMemtableSizeInBytes();
    try {
      this.sstFileManager = new SstFileManager(this.env);
      if (this.memoryLimit > 0) {
        this.sstFileManager.setMaxAllowedSpaceUsage(this.memoryLimit);
        LOGGER.info("Setup the max allowed SST space usage: {} in RocksDB factory", this.memoryLimit);
      }
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to create the shared SstFileManager", e);
    }
    this.rocksDBThrottler = new RocksDBThrottler(rocksDBServerConfig.getDatabaseOpenOperationThrottle());
    this.rateLimiter = new RateLimiter(
        rocksDBServerConfig.getWriteQuotaBytesPerSecond(),
        DEFAULT_REFILL_PERIOD_MICROS,
        DEFAULT_FAIRNESS,
        DEFAULT_MODE,
        rocksDBServerConfig.isAutoTunedRateLimiterEnabled());

  }

  public long getMemoryLimit() {
    return memoryLimit;
  }

  public long getMemtableSize() {
    return memtableSize;
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

  public Cache getSharedCache(boolean isRMD) {
    return rocksDBServerConfig.isUseSeparateRMDCacheEnabled() && isRMD ? sharedRMDCache : sharedCache;
  }

  @Override
  public synchronized AbstractStorageEngine getStorageEngine(VeniceStoreVersionConfig storeConfig)
      throws StorageInitializationException {
    return getStorageEngine(storeConfig, false);
  }

  @Override
  public synchronized AbstractStorageEngine getStorageEngine(
      VeniceStoreVersionConfig storeConfig,
      boolean replicationMetadataEnabled) throws StorageInitializationException {
    verifyPersistenceType(storeConfig);
    final String storeName = storeConfig.getStoreVersionName();
    try {
      return storageEngineMap.computeIfAbsent(
          storeName,
          ignored -> new RocksDBStorageEngine(
              storeConfig,
              this,
              rocksDBPath,
              rocksDBMemoryStats,
              rocksDBThrottler,
              rocksDBServerConfig,
              storeVersionStateSerializer,
              partitionStateSerializer,
              replicationMetadataEnabled));
    } catch (Exception e) {
      throw new StorageInitializationException(e);
    }
  }

  @Override
  public synchronized Set<String> getPersistedStoreNames() {
    File databaseDir = new File(rocksDBPath);
    if (databaseDir.exists() && databaseDir.isDirectory()) {
      String[] storeDirs = databaseDir.list();
      LOGGER.info("Found the following RocksDB databases: {}", Arrays.toString(storeDirs));
      if (storeDirs != null) {
        return new HashSet<>(Arrays.asList(storeDirs));
      }
    } else {
      LOGGER.info("RocksDB dir: {} doesn't exist, so nothing to restore", databaseDir);
    }
    return new HashSet<>();
  }

  @Override
  public synchronized void close() {
    LOGGER.info("Closing RocksDBStorageEngineFactory");
    storageEngineMap.forEach((storeName, storageEngine) -> {
      storageEngine.close();
    });
    storageEngineMap.clear();
    sharedCache.close();
    if (sharedRMDCache != null) {
      sharedRMDCache.close();
    }
    writeBufferManager.close();
    rateLimiter.close();
    this.env.close();
    LOGGER.info("Closed RocksDBStorageEngineFactory");
  }

  @Override
  public synchronized void removeStorageEngine(AbstractStorageEngine engine) {
    verifyPersistenceType(engine);
    final String storeName = engine.getStoreName();
    if (storageEngineMap.containsKey(storeName)) {
      LOGGER.info("Started removing RocksDB storage engine for store: {}", storeName);
      storageEngineMap.get(storeName).drop();
      storageEngineMap.remove(storeName);
      LOGGER.info("Finished removing RocksDB storage engine for store: {}", storeName);
    } else {
      LOGGER.info("RocksDB store: {} doesn't exist", storeName);
    }
  }

  /**
   * The following function shouldn't be invoked at runtime (only at startup time), so we didn't apply
   * any throttling here, and if the above condition changes in the future, we may need to consider
   * throttling the deletion to reduce the IO impact to the database disk, which may result in some
   * side effect in the read path.
   */
  @Override
  public synchronized void removeStorageEngine(String storeName) {
    if (storageEngineMap.containsKey(storeName)) {
      throw new VeniceException(
          "Storage engine has already been opened previously, and please use #removeStorageEngine(AbstractStorageEngine) for deletion");
    }
    File storeDir = new File(rocksDBPath, storeName);
    if (storeDir.exists()) {
      LOGGER.info("Started removing RocksDB database folder for store: {}", storeName);

      try {
        FileUtils.deleteDirectory(storeDir);
      } catch (IOException e) {
        throw new VeniceException("Failed to delete RocksDB database folder for store: " + storeName);
      }

      LOGGER.info("Finished removing RocksDB database folder for store: {}", storeName);
    } else {
      LOGGER.warn("RocksDB store: {} doesn't exist", storeName);
    }
  }

  @Override
  public synchronized void closeStorageEngine(AbstractStorageEngine engine) {
    verifyPersistenceType(engine);
    final String storeName = engine.getStoreName();
    if (storageEngineMap.containsKey(storeName)) {
      LOGGER.info("Started closing RocksDB storage engine for store: {}", storeName);
      storageEngineMap.get(storeName).close();
      storageEngineMap.remove(storeName);
      LOGGER.info("Finished closing RocksDB storage engine for store: {}", storeName);
    } else {
      LOGGER.info("RocksDB store: {} doesn't exist", storeName);
    }
  }

  @Override
  public PersistenceType getPersistenceType() {
    return PersistenceType.ROCKS_DB;
  }
}
