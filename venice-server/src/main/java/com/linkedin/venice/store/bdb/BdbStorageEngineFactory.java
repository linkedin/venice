package com.linkedin.venice.store.bdb;

import com.google.common.collect.Maps;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.server.PartitionAssignmentRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StorageEngineFactory;
import com.linkedin.venice.store.StorageEngineInitializationException;
import com.linkedin.venice.store.memory.InMemoryStorageEngine;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Time;
import com.sleepycat.je.*;
import com.sleepycat.je.rep.impl.node.Feeder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * A BdbStorageEngineFactory contains one or more BDB environments,
 * depending on the config parameter one.env.per.store,
 * false if one env for all stores.
 * <p/>
 * Let's say we have 5 stores and each store has 6 partitions.
 * Schemes:
 * 0. one.env.per.partition = true -> UNSUPPORTED
 * Total Env = 30, Total Db = 30
 * 1. one.env.per.store = true
 * Total Env =  5, Total Db = 30 (6 per store)
 * 2. one.env.per.store = false
 * Total Env =  1, Total Db = 30
 */

public class BdbStorageEngineFactory implements StorageEngineFactory {

  private static final Logger logger = Logger.getLogger(BdbStorageEngineFactory.class.getName());

  private static final String TYPE_NAME = "bdb";
  private static final String SHARED_ENV_KEY = "shared";
  private final Object lock = new Object();

  private final BdbServerConfig bdbServerConfig;
  private final PartitionAssignmentRepository partitionNodeAssignmentRepo;

  private final Map<String, Environment> environments = Maps.newHashMap();
  private final EnvironmentConfig environmentConfig;
  private final String bdbMasterDir;
  private final boolean useOneEnvPerStore;
  private long reservedCacheSize = 0;
  private Set<Environment> unreservedStores;

  // TODO: add aggregated bdb environment stats
  // private AggregatedBdbEnvironmentStats aggBdbStats;

  public BdbStorageEngineFactory(VeniceServerConfig serverConfig,
                                 PartitionAssignmentRepository partitionNodeAssignmentRepo) {
    this.bdbServerConfig = serverConfig.getBdbServerConfig();
    this.partitionNodeAssignmentRepo = partitionNodeAssignmentRepo;

    this.environmentConfig = new EnvironmentConfig().setTransactional(true);

    if (bdbServerConfig.isBdbWriteTransactionsEnabled() && bdbServerConfig.isBdbFlushTransactionsEnabled()) {
      environmentConfig.setDurability(Durability.COMMIT_SYNC);
    } else if (bdbServerConfig.isBdbWriteTransactionsEnabled() && !bdbServerConfig.isBdbFlushTransactionsEnabled()) {
      environmentConfig.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
    } else {
      environmentConfig.setDurability(Durability.COMMIT_NO_SYNC);
    }
    environmentConfig.setAllowCreate(Boolean.TRUE);
    environmentConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX,
      Long.toString(bdbServerConfig.getBdbMaxLogFileSize()));
    environmentConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL,
      Long.toString(bdbServerConfig.getBdbCheckpointBytes()));
    environmentConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_WAKEUP_INTERVAL,
      Long.toString(bdbServerConfig.getBdbCheckpointMs() * Time.US_PER_MS));
    environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_FILE_UTILIZATION,
      Integer.toString(bdbServerConfig.getBdbCleanerMinFileUtilization()));
    environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION,
      Integer.toString(bdbServerConfig.getBdbCleanerMinUtilization()));
    environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_THREADS,
      Integer.toString(bdbServerConfig.getBdbCleanerThreads()));
    environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE,
      Integer.toString(bdbServerConfig.getBdbCleanerLookAheadCacheSize()));
    environmentConfig.setConfigParam(EnvironmentConfig.LOCK_N_LOCK_TABLES,
      Integer.toString(bdbServerConfig.getBdbLockNLockTables()));
    environmentConfig.setConfigParam(EnvironmentConfig.ENV_FAIR_LATCHES,
      Boolean.toString(bdbServerConfig.getBdbFairLatches()));
    environmentConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_HIGH_PRIORITY,
      Boolean.toString(bdbServerConfig.getBdbCheckpointerHighPriority()));
    environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_MAX_BATCH_FILES,
      Integer.toString(bdbServerConfig.getBdbCleanerMaxBatchFiles()));
    environmentConfig.setConfigParam(EnvironmentConfig.LOG_FAULT_READ_SIZE,
      Integer.toString(bdbServerConfig.getBdbLogFaultReadSize()));
    environmentConfig.setConfigParam(EnvironmentConfig.LOG_ITERATOR_READ_SIZE,
      Integer.toString(bdbServerConfig.getBdbLogIteratorReadSize()));
    environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_LAZY_MIGRATION,
      Boolean.toString(bdbServerConfig.getBdbCleanerLazyMigration()));
    environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_BYTES_INTERVAL,
      Long.toString(bdbServerConfig.getBdbCleanerBytesInterval()));
    environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_FETCH_OBSOLETE_SIZE,
      Boolean.toString(bdbServerConfig.getBdbCleanerFetchObsoleteSize()));
    environmentConfig.setLockTimeout(bdbServerConfig.getBdbLockTimeoutMs(), TimeUnit.MILLISECONDS);
    environmentConfig.setConfigParam(EnvironmentConfig.TREE_MAX_DELTA,
      Integer.toString(bdbServerConfig.getBdbMaxDelta()));
    environmentConfig.setConfigParam(EnvironmentConfig.TREE_BIN_DELTA,
      Integer.toString(bdbServerConfig.getBdbBinDelta()));
    environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_ADJUST_UTILIZATION,
      Boolean.toString(bdbServerConfig.getBdbCleanerAdjustUtilization()));
    environmentConfig.setConfigParam(EnvironmentConfig.ENV_RECOVERY_FORCE_CHECKPOINT,
      Boolean.toString(bdbServerConfig.getBdbRecoveryForceCheckpoint()));
    if (bdbServerConfig.getBdbCacheModeEvictLN()) {
      environmentConfig.setCacheMode(CacheMode.EVICT_LN);
    }
    if (bdbServerConfig.isBdbLevelBasedEviction()) {
      environmentConfig.setConfigParam(EnvironmentConfig.EVICTOR_LRU_ONLY,
        Boolean.toString(Boolean.FALSE));
    }

    // Now apply the raw property string overrides
    if (bdbServerConfig.getBdbRawPropertyString() != null) {
      try {
        String[] props = bdbServerConfig.getBdbRawPropertyString().split(",");
        if (props.length > 0) {
          for (int i = 0; i < props.length; i++) {
            String[] propSplit = props[i].split("=");
            if (propSplit.length == 2) {
              logger.info("Overriding property " + propSplit[0] + " to "
                + propSplit[1] + " from the raw property string");
              environmentConfig.setConfigParam(propSplit[0], propSplit[1]);
            }
          }
        }
      } catch (Exception e) {
        logger.warn("Error when applying raw BDB property string... Ignoring and moving on..",
          e);
      }
    }

    bdbMasterDir = serverConfig.getDataBasePath() + File.separator + "bdb";
    useOneEnvPerStore = bdbServerConfig.isBdbOneEnvPerStore();
    unreservedStores = new HashSet<Environment>();

    // TODO: implement aggregated bdb stats and jmx service
    //aggBdbStats = new AggregatedBdbEnvironmentStats();
    //if(veniceConfig.isJmxEnabled()) {
    //  JmxUtils.registerMbean("aggregated-bdb", aggBdbStats);
    //}
  }

  @Override
  public AbstractStorageEngine getStore(VeniceStoreConfig storeConfig) throws StorageInitializationException {
    synchronized (lock) {
      try {
        // TODO: support jmx service
        /*
        if (veniceConfig.isJmxEnabled()) {
          // register the environment stats mbean
          JmxUtils.registerMbean(storeName, engine.getBdbEnvironmentStats());
          // when using a shared environment, there is no meaning to
          // aggregated stats
          if (useOneEnvPerStore) {
            aggBdbStats.trackEnvironment(engine.getBdbEnvironmentStats());
          }
        }
        */

        switch (storeConfig.getPersistenceType()) {
          case BDB:
            Environment environment = getEnvironment(storeConfig);
            return new BdbStorageEngine(storeConfig, partitionNodeAssignmentRepo, environment);
          case IN_MEMORY:
            return new InMemoryStorageEngine(storeConfig, partitionNodeAssignmentRepo);
          case ROCKS_DB:
            throw new NotImplementedException("RocksDB support is not implemented yet!");
          default:
            throw new VeniceException("Persistence type '" + storeConfig.getPersistenceType() + "' is not supported.");
        }

      } catch (Exception e) {
        throw new StorageInitializationException(e);
      }
    }
  }

  @Override
  public String getType() {
    return TYPE_NAME;
  }

  /**
   * Detect what has changed in the store configuration and rewire BDB
   * environments accordingly.
   *
   * @param storeConfig updated store definition
   */
  @Override
  public void update(VeniceStoreConfig storeConfig) {
    if (!useOneEnvPerStore) {
      throw new StorageInitializationException("Memory foot print can be set only when using different environments per store");
    }

    String storeName = storeConfig.getStoreName();
    BdbStoreConfig bdbStoreConfig = storeConfig.getBdbStoreConfig();
    Environment environment = environments.get(storeName);
    // change reservation amount of reserved store
    if (!unreservedStores.contains(environment) && bdbStoreConfig.hasMemoryFootprint()) {
      EnvironmentMutableConfig mConfig = environment.getMutableConfig();
      long currentCacheSize = mConfig.getCacheSize();
      long newCacheSize = bdbStoreConfig.getMemoryFootprintMB() * ByteUtils.BYTES_PER_MB;
      if (currentCacheSize != newCacheSize) {
        long newReservedCacheSize = this.reservedCacheSize - currentCacheSize
          + newCacheSize;

        // check that we leave a 'minimum' shared cache
        if ((bdbServerConfig.getBdbCacheSize() - newReservedCacheSize) < bdbServerConfig.getBdbMinimumSharedCache()) {
          throw new StorageEngineInitializationException("Reservation of "
            + bdbStoreConfig.getMemoryFootprintMB()
            + " MB for store "
            + storeName
            + " violates minimum shared cache size of "
            + bdbServerConfig.getBdbMinimumSharedCache());
        }

        this.reservedCacheSize = newReservedCacheSize;
        adjustCacheSizes();
        mConfig.setCacheSize(newCacheSize);
        environment.setMutableConfig(mConfig);
        logger.info("Setting private cache for store " + storeConfig.getStoreName() + " to "
          + newCacheSize);
      }
    } else {
      // we cannot support changing a reserved store to unreserved or vice
      // versa since the sharedCache param is not mutable
      throw new StorageInitializationException("Cannot switch between shared and private cache dynamically");
    }
  }

  @Override
  public void close() {
    synchronized (lock) {
      try {
        for (Environment environment : environments.values()) {
          environment.sync();
          environment.close();
        }
      } catch (DatabaseException e) {
        throw new VeniceException(e);
      }
    }
  }

  /**
   * Clean up the environment object for the given storage engine
   */
  @Override
  public void removeStorageEngine(AbstractStorageEngine engine) {
    String storeName = engine.getName();

    // TODO: support jmx service
    //BdbStorageEngine bdbEngine = (BdbStorageEngine) engine;

    synchronized (lock) {

      // Only cleanup the environment if it is per store. We cannot
      // cleanup a shared 'Environment' object
      if (useOneEnvPerStore) {

        Environment environment = this.environments.get(storeName);
        if (environment == null) {
          // Nothing to clean up.
          return;
        }

        // Remove from the set of unreserved stores if needed.
        if (this.unreservedStores.remove(environment)) {
          logger.info("Removed environment for store name: " + storeName
            + " from unreserved stores");
        } else {
          logger.info("No environment found in unreserved stores for store name: "
            + storeName);
        }

        // Try to delete the BDB directory associated
        File bdbDir = environment.getHome();
        if (bdbDir.exists() && bdbDir.isDirectory()) {
          String bdbDirPath = bdbDir.getPath();
          try {
            FileUtils.deleteDirectory(bdbDir);
            logger.info("Successfully deleted BDB directory : " + bdbDirPath
              + " for store name: " + storeName);
          } catch (IOException e) {
            logger.error("Unable to delete BDB directory: " + bdbDirPath
              + " for store name: " + storeName);
          }
        }

        // TODO: add bdb environment stats here
        // Remove the reference to BdbEnvironmentStats, which holds a
        // reference to the Environment
        //BdbEnvironmentStats bdbEnvStats = bdbEngine.getBdbEnvironmentStats();
        //this.aggBdbStats.unTrackEnvironment(bdbEnvStats);

        // Unregister the JMX bean for Environment
        // TODO: add jmx service here
        /*
        if (veniceConfig.isJmxEnabled()) {
          ObjectName name = JmxUtils.createObjectName(JmxUtils.getPackageName(bdbEnvStats.getClass()),
            storeName);
          // Un-register the environment stats mbean
          JmxUtils.unregisterMbean(name);
        }
        */

        // Cleanup the environment
        environment.close();
        this.environments.remove(storeName);
        logger.info("Successfully closed the environment for store name : " + storeName);
      }
    }
  }

  public Environment getEnvironment(VeniceStoreConfig storeConfig) throws DatabaseException {

    String storeName = storeConfig.getStoreName();
    BdbStoreConfig bdbStoreConfig = storeConfig.getBdbStoreConfig();

    synchronized (lock) {
      if (useOneEnvPerStore) {
        // if we have already created this environment return a
        // reference
        if (environments.containsKey(storeName))
          return environments.get(storeName);

        // otherwise create a new environment
        BdbServerConfig bdbServerConfig = storeConfig.getBdbServerConfig();
        File bdbDir = new File(bdbMasterDir, storeName);
        createBdbDirIfNecessary(bdbDir);

        // configure the BDB cache
        if (bdbStoreConfig.hasMemoryFootprint()) {
          // make room for the reservation, by adjusting other stores
          long reservedBytes = bdbStoreConfig.getMemoryFootprintMB() * ByteUtils.BYTES_PER_MB;
          long newReservedCacheSize = this.reservedCacheSize + reservedBytes;

          // check that we leave a 'minimum' shared cache
          if ((bdbServerConfig.getBdbCacheSize() - newReservedCacheSize) < bdbServerConfig.getBdbMinimumSharedCache()) {
            throw new StorageEngineInitializationException("Reservation of "
              + bdbStoreConfig.getMemoryFootprintMB()
              + " MB for store "
              + storeName
              + " violates minimum shared cache size of "
              + bdbServerConfig.getBdbMinimumSharedCache());
          }

          this.reservedCacheSize = newReservedCacheSize;
          adjustCacheSizes();
          environmentConfig.setSharedCache(false);
          environmentConfig.setCacheSize(reservedBytes);
        } else {
          environmentConfig.setSharedCache(true);
          environmentConfig.setCacheSize(bdbServerConfig.getBdbCacheSize()
            - this.reservedCacheSize);
        }

        Environment environment = new Environment(bdbDir, environmentConfig);
        logger.info("Creating environment for " + storeName + ": ");
        logEnvironmentConfig(environment.getConfig());
        environments.put(storeName, environment);

        // save this up so we can adjust later if needed
        if (!bdbStoreConfig.hasMemoryFootprint())
          this.unreservedStores.add(environment);

        return environment;
      } else {
        if (!environments.isEmpty())
          return environments.get(SHARED_ENV_KEY);

        File bdbDir = new File(bdbMasterDir);
        createBdbDirIfNecessary(bdbDir);

        Environment environment = new Environment(bdbDir, environmentConfig);
        logger.info("Creating shared BDB environment: ");
        logEnvironmentConfig(environment.getConfig());
        environments.put(SHARED_ENV_KEY, environment);
        return environment;
      }
    }
  }

  private void createBdbDirIfNecessary(File bdbDir) {
    if (!bdbDir.exists()) {
      logger.info("Creating BDB data directory '" + bdbDir.getAbsolutePath() + ".");
      bdbDir.mkdirs();
    }
  }

  /**
   * When a reservation is made, we need to shrink the shared cache
   * accordingly to guarantee memory foot print of the new store. NOTE: This
   * is not an instantaeneous operation. Changes will take effect only when
   * traffic is thrown and eviction happens.( Won't happen until Network ports
   * are opened anyway which is rightfully done after storage service).When
   * changing this dynamically, we might want to block until the shared cache
   * shrinks enough
   */
  private void adjustCacheSizes() {
    long newSharedCacheSize = bdbServerConfig.getBdbCacheSize() - this.reservedCacheSize;
    logger.info("Setting the shared cache size to " + newSharedCacheSize);
    for (Environment environment : unreservedStores) {
      EnvironmentMutableConfig mConfig = environment.getMutableConfig();
      mConfig.setCacheSize(newSharedCacheSize);
      environment.setMutableConfig(mConfig);
    }
  }

  private void logEnvironmentConfig(EnvironmentConfig config) {
    logger.info("    BDB cache size = " + config.getCacheSize());
    logger.info("    BDB " + EnvironmentConfig.CLEANER_THREADS + " = "
      + config.getConfigParam(EnvironmentConfig.CLEANER_THREADS));
    logger.info("    BDB " + EnvironmentConfig.CLEANER_MIN_UTILIZATION + " = "
      + config.getConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION));
    logger.info("    BDB " + EnvironmentConfig.CLEANER_MIN_FILE_UTILIZATION + " = "
      + config.getConfigParam(EnvironmentConfig.CLEANER_MIN_FILE_UTILIZATION));
    logger.info("    BDB " + EnvironmentConfig.LOG_FILE_MAX + " = "
      + config.getConfigParam(EnvironmentConfig.LOG_FILE_MAX));
    logger.info("    BDB " + config.toString().replace('\n', ','));
  }

  public String getStats(String storeName, boolean fast) {
    try {
      if (environments.containsKey(storeName)) {
        StatsConfig config = new StatsConfig();
        config.setFast(fast);
        Environment env = environments.get(storeName);
        return env.getStats(config).toString();
      } else {
        // return empty string if environment not created yet
        return "";
      }
    } catch (DatabaseException e) {
      throw new VeniceException(e);
    }
  }

  public String getEnvStatsAsString(String storeName, boolean fast) throws Exception {
    String envStats = getStats(storeName, fast);
    logger.debug("Bdb Environment stats:\n" + envStats);
    return envStats;
  }

  public String getEnvStatsAsString(String storeName) throws Exception {
    return getEnvStatsAsString(storeName, true);
  }

  /**
   * Forceful cleanup the logs
   */
  public void cleanLogs() {
    synchronized (lock) {
      try {
        for (Environment environment : environments.values()) {
          environment.cleanLog();
        }
      } catch (DatabaseException e) {
        throw new VeniceException(e);
      }
    }
  }

  /**
   * Forceful checkpointing
   */
  public void checkpointAllEnvironments() {
    synchronized (lock) {
      try {
        for (Environment environment : environments.values()) {
          CheckpointConfig checkPointConfig = new CheckpointConfig();
          checkPointConfig.setForce(true);
          environment.checkpoint(checkPointConfig);
        }
      } catch (DatabaseException e) {
        throw new VeniceException(e);
      }
    }
  }

  public long getReservedCacheSize() {
    return this.reservedCacheSize;
  }
}
