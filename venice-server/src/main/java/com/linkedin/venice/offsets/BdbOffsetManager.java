package com.linkedin.venice.offsets;

import com.linkedin.venice.config.VeniceClusterConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Time;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.io.File;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;


public class BdbOffsetManager extends OffsetManager {

  /**
   * This is where we can decide one of the two models:
   * 1. A single metadata database per store ( the number of records in this database will be the number of
   * partitions served by this node for that store)
   * 2. (OR) one single metadata database for all stores( here the number of records in the database will be the total
   * number of partitions served by this node)
   *
   * We will be going with approach 2. A new environment is created and passed on to the BDBStoragePartition
   * class that will open or create a bdb database as needed. This database will be referenced as the
   * OffsetMetadataStore and is local to this node. Please note that this is not a regular Venice Store. A router or
   * an admin service would need to know the node id to query the metadata on that node.
   *
   * The config properties for this bdb environment & database need to be tweaked to support the offset storage and
   * persistence requirements.
   *
   */
  private static final long CHECKPOINTER_BYTES_INTERVAL = 0L; // To enable time based checkpointing
  private static final long LOG_FILE_MAX = 1L * 1024L * 1024L;// 1MB log file is more than enough and is the minimum
  private static final long CACHE_SIZE = 1024L * 1024L; // 1MB cache   TODO increase this later if needed

  private static final Logger logger = Logger.getLogger(BdbOffsetManager.class.getName());
  private final AtomicBoolean isOpen;
  private final Environment offsetsBdbEnvironment;
  private final Database offsetsBdbDatabase;

  // TODO: Need to remove this later - start
  private final HashMap<String, Integer> consumptionStats;
  // TODO: Need to remove this later - end

  public BdbOffsetManager(VeniceClusterConfig veniceClusterConfig) {
    super(veniceClusterConfig);

    String bdbMasterDir = veniceClusterConfig.getOffsetDatabasePath();
    File bdbDir = new File(bdbMasterDir);
    if (!bdbDir.exists()) {
      logger.info("Creating BDB data directory '" + bdbDir.getAbsolutePath() + ".");
      bdbDir.mkdirs();
    }

    EnvironmentConfig envConfig = new EnvironmentConfig();
    envConfig.setAllowCreate(true);
    envConfig.setTransactional(false);
    envConfig.setReadOnly(false);
    envConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL, Long.toString(CHECKPOINTER_BYTES_INTERVAL));
    envConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_WAKEUP_INTERVAL,
        Long.toString(veniceClusterConfig.getOffsetManagerFlushIntervalMs() * Time.US_PER_MS));
    envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, Long.toString(LOG_FILE_MAX));
    envConfig.setConfigParam(EnvironmentConfig.ENV_RECOVERY_FORCE_CHECKPOINT, Boolean.toString(true));
    envConfig.setCacheSize(CACHE_SIZE);

    DatabaseConfig dbConfig = new DatabaseConfig();
    dbConfig.setAllowCreate(true);
    dbConfig.setSortedDuplicates(false);
    dbConfig.setDeferredWrite(true);
    dbConfig.setTransactional(false);

    offsetsBdbEnvironment = new Environment(bdbDir, envConfig);
    logger.info("Creating BDB environment for storing offsets: ");
    this.offsetsBdbDatabase = offsetsBdbEnvironment.openDatabase(null, OFFSETS_STORE_NAME, dbConfig);
    this.isOpen = new AtomicBoolean(true);

    // TODO: Need to remove this block later - start
    consumptionStats = new HashMap<String, Integer>();
    // TODO: Need to remove this block later - end
  }

  @Override
  public void shutdown()
      throws VeniceException {
    try {
      if (this.isOpen.compareAndSet(true, false)) {
        this.offsetsBdbEnvironment.sync();
        this.offsetsBdbDatabase.close();
        this.offsetsBdbEnvironment.close();
      }
    } catch (DatabaseException e) {
      logger.error(e);
      throw new VeniceException("Shutdown failed for BDB database " + OFFSETS_STORE_NAME, e);
    }
  }

  @Override
  public void recordOffset(String topicName, int partitionId, OffsetRecord record)
      throws VeniceException {
    //assumes that the offset is not negative. Checked by the caller
    String keyStr = topicName + "_" + partitionId;
    byte[] value = record.toBytes();

    DatabaseEntry keyEntry = new DatabaseEntry(keyStr.getBytes());
    DatabaseEntry valueEntry = new DatabaseEntry(value);
    try {
      OperationStatus status = offsetsBdbDatabase.put(null, keyEntry, valueEntry);

      if (status != OperationStatus.SUCCESS) {
        String errorStr = "Put failed with  " + status + " for key " + keyStr;
        logger.error(errorStr);
        throw new VeniceException(errorStr);
      }

      // TODO: Need to remove this block later - start
      if (!consumptionStats.containsKey(keyStr)) {
        consumptionStats.put(keyStr, 1);
        logger.info(keyStr + ":" + record);
        offsetsBdbDatabase.sync();
      } else {
        int val = consumptionStats.get(keyStr);
        if (val + 1 == 50) {
          logger.info(keyStr + ":" + record);
          offsetsBdbDatabase.sync();
          consumptionStats.put(keyStr, 0);
        } else {
          consumptionStats.put(keyStr, val + 1);
        }
      }
      // TODO: Need to remove this block later - end
    } catch (DatabaseException e) {
      logger.error("Error in put for BDB database " + OFFSETS_STORE_NAME, e);
      throw new VeniceException(e);
    }
  }

  @Override
  public OffsetRecord getLastOffset(String topicName, int partitionId)
      throws VeniceException {

    /**
     * This method will return null if last offset is not found.
     */
    String keyStr = topicName + "_" + partitionId;

    DatabaseEntry keyEntry = new DatabaseEntry(keyStr.getBytes());
    DatabaseEntry valueEntry = new DatabaseEntry();

    try {
      OperationStatus status = offsetsBdbDatabase.get(null, keyEntry, valueEntry, LockMode.READ_UNCOMMITTED);
      if (OperationStatus.SUCCESS == status) {
        return new OffsetRecord(valueEntry.getData(), 0);
      } else {
        // case when the key (topic,partition)  does not exist
        return null;
      }
    } catch (DatabaseException e) {
      logger.error(e);
      throw new VeniceException(e);
    }
  }

}
