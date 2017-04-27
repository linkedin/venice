package com.linkedin.venice.offsets;

import com.linkedin.venice.config.VeniceClusterConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.service.AbstractVeniceService;
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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;


public class BdbOffsetManager extends AbstractVeniceService implements OffsetManager  {

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
  public static final String OFFSETS_STORE_NAME="offsets_store";

  private static final long CHECKPOINTER_BYTES_INTERVAL = 0L; // To enable time based checkpointing
  private static final long LOG_FILE_MAX = 1L * 1024L * 1024L;// 1MB log file is more than enough and is the minimum
  private static final long CACHE_SIZE = 1024L * 1024L; // 1MB cache   TODO increase this later if needed

  private static final Logger logger = Logger.getLogger(BdbOffsetManager.class);
  private final Environment offsetsBdbEnvironment;

  private AtomicBoolean isOpen;
  private Database offsetsBdbDatabase;

  @Override
  public boolean startInner()
          throws Exception {
    DatabaseConfig dbConfig = new DatabaseConfig();
    dbConfig.setAllowCreate(true);
    dbConfig.setSortedDuplicates(false);
    // This is required since by default DatabaseConfig is not transactional.
    dbConfig.setTransactional(true);

    logger.info("Creating BDB environment for storing offsets: ");
    this.offsetsBdbDatabase = offsetsBdbEnvironment.openDatabase(null, OFFSETS_STORE_NAME, dbConfig);
    this.isOpen = new AtomicBoolean(true);

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  public BdbOffsetManager(VeniceClusterConfig veniceClusterConfig) {
    String bdbMasterDir = veniceClusterConfig.getOffsetDatabasePath();
    File bdbDir = new File(bdbMasterDir);
    if (!bdbDir.exists()) {
      logger.info("Creating BDB data directory '" + bdbDir.getAbsolutePath() + ".");
      bdbDir.mkdirs();
    }

    EnvironmentConfig envConfig = new EnvironmentConfig();
    envConfig.setAllowCreate(true);
    envConfig.setReadOnly(false);
    // This is required since by default EnvironmentConfig is not transactional.
    envConfig.setTransactional(true);
    envConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL, Long.toString(CHECKPOINTER_BYTES_INTERVAL));
    envConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_WAKEUP_INTERVAL,
            Long.toString(veniceClusterConfig.getOffsetManagerFlushIntervalMs() * Time.US_PER_MS));
    envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, Long.toString(LOG_FILE_MAX));
    envConfig.setConfigParam(EnvironmentConfig.ENV_RECOVERY_FORCE_CHECKPOINT, Boolean.toString(true));
    envConfig.setCacheSize(CACHE_SIZE);

    offsetsBdbEnvironment = new Environment(bdbDir, envConfig);
  }

  @Override
  public void stopInner()
      throws VeniceException {
    try {
      if (this.isOpen.compareAndSet(true, false)) {
        this.offsetsBdbEnvironment.sync();
        this.offsetsBdbDatabase.close();
        // This will make sure the 'cleaner thread' will be shutdown properly.
        this.offsetsBdbEnvironment.cleanLog();
        this.offsetsBdbEnvironment.close();
      }
    } catch (DatabaseException e) {
      logger.error(e);
      throw new VeniceException("Shutdown failed for BDB database " + OFFSETS_STORE_NAME, e);
    }
  }

  private DatabaseEntry getBDBKey(String topic, int partitionId) {
    String keyStr = topic + "_" + partitionId;
    byte[] key = keyStr.getBytes(StandardCharsets.UTF_8);
    return new DatabaseEntry(key);
  }

  @Override
  public void recordOffset(String topicName, int partitionId, OffsetRecord record)
      throws VeniceException {

    DatabaseEntry keyEntry = getBDBKey(topicName, partitionId);

    byte[] value = record.toBytes();
    DatabaseEntry valueEntry = new DatabaseEntry(value);

    try {
      OperationStatus status = offsetsBdbDatabase.put(null, keyEntry, valueEntry);

      if (status != OperationStatus.SUCCESS) {
        String errorStr = "Put failed with  " + status + " for Topic: " + topicName + " PartitionId: " + partitionId;
        logger.error(errorStr);
        throw new VeniceException(errorStr);
      }
    } catch (DatabaseException e) {
      String errorMessage = "Error in put for BDB database " + OFFSETS_STORE_NAME +
          " for Topic: " + topicName + " PartitionId: " + partitionId;
      logger.error(errorMessage , e);
      throw new VeniceException(e);
    }
  }

  @Override
  public void clearOffset(String topicName, int partitionId) {
    DatabaseEntry keyEntry = getBDBKey(topicName, partitionId);

    try {
      OperationStatus status = offsetsBdbDatabase.delete(null, keyEntry);

      switch (status) {
        case SUCCESS:
          logger.info("Successfully deleted offset data for topic '" + topicName + "', partition " + partitionId);
          break;
        case NOTFOUND:
          logger.warn("Attempted to delete offset data but it was already missing for topic '" + topicName +
              "', partition " + partitionId);
          break;
        default:
          throw new VeniceException("Failed to delete offset data with status '" + status + "' for topic '" +
              topicName + "', partitionId " + partitionId);
      }
    } catch (DatabaseException e) {
      String errorMessage = "Error in clearOffset for BDB database " + OFFSETS_STORE_NAME +
          " Topic " + topicName + " PartitionId: " + partitionId;
      logger.error(errorMessage , e);
      throw new VeniceException(errorMessage, e);
    }
  }

  @Override
  public OffsetRecord getLastOffset(String topicName, int partitionId)
      throws VeniceException {
    /**
     * This method will return null if last offset is not found.
     */
    DatabaseEntry keyEntry = getBDBKey(topicName , partitionId);
    DatabaseEntry valueEntry = new DatabaseEntry();

    try {
      OperationStatus status = offsetsBdbDatabase.get(null, keyEntry, valueEntry, LockMode.READ_UNCOMMITTED);
      if (OperationStatus.SUCCESS == status) {
        return new OffsetRecord(valueEntry.getData());
      } else {
        // case when the key (topic,partition)  does not exist
        return new OffsetRecord();
      }
    } catch (DatabaseException e) {
      String errorMessage = "Error retrieving offset for Topic " + topicName + " and partition" + partitionId;
      logger.error(errorMessage, e);
      throw new VeniceException(errorMessage, e);
    }
  }
}
