package com.linkedin.venice.store.rocksdb;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.exceptions.VeniceException;
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
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

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
   * Shared Options across all the RocksDB databases.
   * The reason to use shared {@link Options} is that that is the only way to share block cache
   * across all the RocksDB databases.
   * Once the following code change gets released:
   * https://github.com/facebook/rocksdb/commit/d5585bb605824e33a47359714b04b7bf14c0a2f6
   * We could use different {@link Options} for different databases.
   */
  private final Options options;

  /**
   * RocksDB root path
   */
  private final String rocksDBPath;

  private final Map<String, RocksDBStorageEngine> storageEngineMap = new HashMap<>();

  public RocksDBStorageEngineFactory(VeniceServerConfig serverConfig) {
    this.rocksDBServerConfig = serverConfig.getRocksDBServerConfig();

    this.rocksDBPath = serverConfig.getDataBasePath() + File.separator + "rocksdb";

    /**
     * Shared {@link Env} allows us to share the flush thread pool and compaction thread pool.
     */
    this.env = Env.getDefault();
    // Make them configurable
    this.env.setBackgroundThreads(rocksDBServerConfig.getRocksDBEnvFlushPoolSize(), FLUSH_POOL);
    this.env.setBackgroundThreads(rocksDBServerConfig.getRocksDBEnvCompactionPoolSize(), COMPACTION_POOL);
    this.options = new Options();
    this.options.setEnv(this.env);
    this.options.setCreateIfMissing(true);
    this.options.setCompressionType(rocksDBServerConfig.getRocksDBOptionsCompressionType());
    this.options.setCompactionStyle(rocksDBServerConfig.getRocksDBOptionsCompactionStyle());
    this.options.setBytesPerSync(rocksDBServerConfig.getRocksDBBytesPerSync());

    // Cache index and bloom filter in block cache
    // and share the same cache across all the RocksDB databases
    BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
    tableConfig.setBlockSize(rocksDBServerConfig.getRocksDBSSTFileBlockSizeInBytes());
    tableConfig.setBlockCacheSize(rocksDBServerConfig.getRocksDBBlockCacheSizeInBytes());
    tableConfig.setCacheIndexAndFilterBlocks(true);
    tableConfig.setBlockCacheCompressedSize(rocksDBServerConfig.getRocksDBBlockCacheCompressedSizeInBytes());
    tableConfig.setFormatVersion(2); // Latest version

    this.options.setTableFormatConfig(tableConfig);

    // Memtable options
    this.options.setWriteBufferSize(rocksDBServerConfig.getRocksDBMemtableSizeInBytes());
    this.options.setMaxWriteBufferNumber(rocksDBServerConfig.getRocksDBMaxMemtableCount());
    this.options.setMaxTotalWalSize(rocksDBServerConfig.getRocksDBMaxTotalWalSizeInBytes());
    this.options.setMaxBytesForLevelBase(rocksDBServerConfig.getRocksDBMaxBytesForLevelBase());
  }

  @Override
  public synchronized AbstractStorageEngine getStore(VeniceStoreConfig storeConfig) throws StorageInitializationException {
    verifyPersistenceType(storeConfig);
    final String storeName = storeConfig.getStoreName();
    try {
      if (!storageEngineMap.containsKey(storeName)) {
        storageEngineMap.put(storeName, new RocksDBStorageEngine(storeConfig, options, rocksDBPath));
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
    this.options.close();
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