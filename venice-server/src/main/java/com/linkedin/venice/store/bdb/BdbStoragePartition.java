package com.linkedin.venice.store.bdb;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.store.AbstractStoragePartition;
import com.linkedin.venice.store.StoragePartitionConfig;
import com.linkedin.venice.store.exception.InvalidDatabaseNameException;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.partition.iterators.AbstractCloseablePartitionEntriesIterator;
import com.linkedin.venice.utils.partition.iterators.CloseablePartitionKeysIterator;
import com.sleepycat.je.*;
import java.util.Collections;
import java.util.Map;
import java.nio.ByteBuffer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.linkedin.venice.meta.PersistenceType.*;

/**
 * A BDB storage partition, essentially a BDB database
 * <p/>
 * Assumptions:
 * 1. No need to worry about synchronizing write/deletes as the model is based on a single writer.
 * So all updates are already serialized.
 * 2. Concurrent reads may be stale if writes/deletes are going on. But the consistency model is also designed to be eventual.
 * Since "read your own writes semantics" is not guaranteed this eventual consistency is tolerable.
 * <p/>
 * Even though there will be one writer and 1 or more readers, we may still need a concurrentHashMap to avoid
 * ConcurrentModificationException thrown from the iterators
 */
public class BdbStoragePartition extends AbstractStoragePartition {
  private static final Logger logger = Logger.getLogger(BdbStoragePartition.class);
  private static final String DATABASE_NAME_SEPARATOR = "-";

  private final String storeName;
  private final Environment environment;
  private Database database;

  private final DatabaseConfig databaseConfig;
  private final LockMode readLockMode;
  private final boolean minimizeScanImpact;

  private final AtomicBoolean isOpen;
  private final AtomicBoolean isTruncating = new AtomicBoolean(false);

  private final boolean checkpointAfterDropping;

  public BdbStoragePartition(StoragePartitionConfig storagePartitionConfig, Environment environment, BdbServerConfig bdbServerConfig) {
    super(storagePartitionConfig.getPartitionId());

    this.storeName = Utils.notNull(storagePartitionConfig.getStoreName());
    this.environment = Utils.notNull(environment);

    this.databaseConfig = new DatabaseConfig();
    this.databaseConfig.setAllowCreate(true);
    this.databaseConfig.setSortedDuplicates(false);
    this.databaseConfig.setNodeMaxEntries(bdbServerConfig.getBdbBtreeFanout());
    this.databaseConfig.setKeyPrefixing(bdbServerConfig.isBdbDatabaseKeyPrefixing());
    if (bdbServerConfig.isBdbDatabaseKeyPrefixing()) {
      logger.info("Opening database for store: " + getBdbDatabaseName() + " with key-prefixing enabled");
    }
    if (storagePartitionConfig.isDeferredWrite()) {
      // Non-transactional
      this.databaseConfig.setDeferredWrite(true);
      logger.info("Opening database for store: " + getBdbDatabaseName() + " in deferred-write mode");
    } else {
      this.databaseConfig.setTransactional(true);
      logger.info("Opening database for store: " + getBdbDatabaseName() + " in transactional mode");
    }
    this.checkpointAfterDropping = bdbServerConfig.isBdbCheckpointAfterDropping();

    this.database = this.environment.openDatabase(null, getBdbDatabaseName(), databaseConfig);
    // Sync here to make sure the new database will be persisted.
    this.environment.sync();

    BdbRuntimeConfig bdbRuntimeConfig = new BdbRuntimeConfig(bdbServerConfig);
    this.readLockMode = bdbRuntimeConfig.getLockMode();
    this.minimizeScanImpact = bdbRuntimeConfig.getMinimizeScanImpact();

    this.isOpen = new AtomicBoolean(true);
  }

  private void put(DatabaseEntry keyEntry, DatabaseEntry valueEntry) throws VeniceException {
    boolean succeeded = false;
    long startTimeNs = -1;

    if (logger.isTraceEnabled())
      startTimeNs = System.nanoTime();
    try {
      /* TODO: decide where to add code for conflict resolution
         if it is in propagation layer, then remove this comment;
         otherwise read entry here and modify it */

      /**
       * By default, 'auto-commit' will be used if it is a  transactional database.
       * https://docs.oracle.com/cd/E17277_02/html/TransactionGettingStarted/autocommit.html
       */
      OperationStatus status = getBdbDatabase().put(null, keyEntry, valueEntry);

      if (status != OperationStatus.SUCCESS) {
        throw new VeniceException("Put operation failed with status: " + status + " Store " + getBdbDatabaseName() );
      }

      succeeded = true;
    } catch (DatabaseException e) {
      //this.bdbEnvironmentStats.reportException(e);
      logger.error("Error in put for BDB database " + getBdbDatabaseName(), e);
      throw new VeniceException(e);
    } finally {
      if (logger.isTraceEnabled()) {
        byte[] key = keyEntry.getData();
        logger.trace(succeeded ? "Successfully completed" : "Failed to complete"
            + " PUT (" + getBdbDatabaseName() + ") to key " + key + " (keyRef: "
            + System.identityHashCode(key) + " in "
            + (System.nanoTime() - startTimeNs) + " ns at "
            + System.currentTimeMillis());
      }
    }
  }

  public void put(byte[] key, byte[] value) throws VeniceException {
    put(new DatabaseEntry(key), new DatabaseEntry(value));
  }

  @Override
  public void put(byte[] key, ByteBuffer value) {
    put(new DatabaseEntry(key), new DatabaseEntry(value.array(), value.position(), value.remaining()));
  }

  private byte[] get(DatabaseEntry keyEntry) {
    DatabaseEntry valueEntry = new DatabaseEntry();

    try {
      // uncommitted reads are perfectly fine now, since we have no
      // je-delete() in put()
      OperationStatus status = getBdbDatabase().get(null, keyEntry, valueEntry, readLockMode);
      if (OperationStatus.SUCCESS == status) {
        return valueEntry.getData();
      } else {
        return null;
      }
    } catch (DatabaseException e) {
      //this.bdbEnvironmentStats.reportException(e);
      logger.error(e);
      throw new VeniceException(e);
    }
  }

  public byte[] get(byte[] key) throws VeniceException {
    return get(new DatabaseEntry(key));
  }

  @Override
  public byte[] get(ByteBuffer keyBuffer) {
    return get(new DatabaseEntry(keyBuffer.array(), keyBuffer.position(), keyBuffer.remaining()));
  }

  public void delete(byte[] key) {
    boolean succeeded = false;
    long startTimeNs = -1;

    if (logger.isTraceEnabled()) {
      startTimeNs = System.nanoTime();
    }

    try {
      DatabaseEntry keyEntry = new DatabaseEntry(key);

      /**
       * By default, 'auto-commit' will be used if it is a  transactional database.
       * https://docs.oracle.com/cd/E17277_02/html/TransactionGettingStarted/autocommit.html
       */
      getBdbDatabase().delete(null, keyEntry);
      succeeded = true;
    } catch (DatabaseException e) {
      //this.bdbEnvironmentStats.reportException(e);
      logger.error(e);
      throw new VeniceException(e);
    } finally {
      if (logger.isTraceEnabled()) {
        logger.trace(succeeded ? "Successfully completed" : "Failed to complete"
          + " DELETE (" + getBdbDatabaseName() + ") of key "
          + ByteUtils.toLogString(key) + " (keyRef: "
          + System.identityHashCode(key) + ") in "
          + (System.nanoTime() - startTimeNs) + " ns at "
          + System.currentTimeMillis());
      }
    }
  }

  /**
   * Get an iterator over pairs of entries in the partition. The key is the first
   * element in the pair and the value is the second element.
   * <p/>
   * Note that the iterator need not be threadsafe, and that it must be
   * manually closed after use.
   *
   * This function is kept since it could be used for BDB->RocksDB migration purpose.
   *
   * @return An iterator over the entries in this AbstractStoragePartition.
   */
  public AbstractCloseablePartitionEntriesIterator partitionEntries() {
    try {
      Cursor cursor = getBdbDatabase().openCursor(null, null);
      // evict data brought in by the cursor walk right away
      if (this.minimizeScanImpact) {
        cursor.setCacheMode(CacheMode.EVICT_BIN);
      }

      return new ClosableBdbPartitionEntriesIterator(cursor, this);
    } catch (DatabaseException e) {
      //this.bdbEnvironmentStats.reportException(e);
      logger.error(e);
      throw new VeniceException(e);
    }
  }

  /**
   * /**
   * Get an iterator over keys in the partition.
   * <p/>
   * Note that the iterator need not be threadsafe, and that it must be
   * manually closed after use.
   *
   * This function is kept since it could be used for BDB->RocksDB migration purpose.
   *
   * @return An iterator over the keys in this AbstractStoragePartition.
   */
  public CloseablePartitionKeysIterator partitionKeys() {
    return new CloseablePartitionKeysIterator(partitionEntries());
  }


  interface BDBOperation {
    void perform();
  }

  private synchronized  void performOperation(String operationType, BDBOperation operation) {
    try {
      // close current bdbDatabase first
      database.close();
      /**
       * By default, 'auto-commit' will be used if it is a  transactional database.
       * https://docs.oracle.com/cd/E17277_02/html/TransactionGettingStarted/autocommit.html
       */
      operation.perform();
    } catch (DatabaseException e) {
      String errorMessage = "Failed to " + operationType + " BDB database " + getBdbDatabaseName();
      logger.error(errorMessage, e);
      throw new VeniceException(errorMessage , e);
    }

  }

  /**
   * Drop the BDB database, when it is not required anymore.
   */
  @Override
  public synchronized void drop() {
    BDBOperation dropDB =() -> environment.removeDatabase(null, getBdbDatabaseName());
    performOperation("DROP" , dropDB );
    if (checkpointAfterDropping) {
      // Log file deletion only occurs after a checkpoint
      environment.cleanLog();
      CheckpointConfig ckptConfig = new CheckpointConfig();
      ckptConfig.setMinimizeRecoveryTime(true);
      ckptConfig.setForce(true);
      environment.checkpoint(ckptConfig);
    }
  }

  @Override
  public Map<String, String> sync() {
    if (databaseConfig.getDeferredWrite()) {
      // Only applicable for deferred-write database.
      database.sync();
    }
    return Collections.emptyMap();

  }

  @Override
  public void close() {
    try {
      if (this.isOpen.compareAndSet(true, false))
        this.getBdbDatabase().close();
    } catch (DatabaseException e) {
      // this.bdbEnvironmentStats.reportException(e);
      logger.error(e);
      throw new VeniceException("Shutdown failed for BDB database " + getBdbDatabaseName(), e);
    }
  }

  @Override
  public boolean verifyConfig(StoragePartitionConfig storagePartitionConfig) {
    boolean databaseDeferredWrite = databaseConfig.getDeferredWrite();
    boolean partitionConfigDeferredWrite = storagePartitionConfig.isDeferredWrite();
    return (databaseDeferredWrite == partitionConfigDeferredWrite);
  }

  private class ClosableBdbPartitionEntriesIterator extends AbstractCloseablePartitionEntriesIterator {

    private volatile boolean isOpen;
    final Cursor cursor;
    final BdbStoragePartition partition;

    ClosableBdbPartitionEntriesIterator(Cursor cursor, BdbStoragePartition partition) {
      this.isOpen = true;
      this.cursor = cursor;
      this.partition = partition;
    }

    @Override
    public void close() throws IOException {
      try {
        if (isOpen) {
          cursor.close();
          isOpen = false;
        }
      } catch (DatabaseException e) {
        logger.error(e);
      }
    }

    @Override
    public boolean fetchNextEntry() {
      DatabaseEntry keyEntry = new DatabaseEntry();
      DatabaseEntry valueEntry = new DatabaseEntry();
      try {
        OperationStatus status = cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED);

        if (OperationStatus.NOTFOUND == status) {
          // we have reached the end of the cursor
          return false;
        }
        this.currentEntry = new AbstractMap.SimpleEntry(keyEntry.getData(), valueEntry.getData());
        return true;
      } catch (DatabaseException e) {
        // partition.bdbEnvironmentStats.reportException(e);
        logger.error(e);
        throw new VeniceException(e);
      }
    }

    @Override
    public void remove() {
      //To change body of implemented methods use File | Settings | File Templates.
    }
  }

  private String getBdbDatabaseName() {
    return storeName + DATABASE_NAME_SEPARATOR + partitionId;
  }


  private static int getSeparatorIndex(String partitionName) {
    int index = partitionName.lastIndexOf(DATABASE_NAME_SEPARATOR);
    if (-1 == index) {
      throw new InvalidDatabaseNameException(BDB, partitionName);
    }
    if (0 == index || partitionName.length() - 1 == index) {
      throw new InvalidDatabaseNameException(BDB, partitionName);
    }
    return index;
  }

  public static String getStoreNameFromPartitionName(String partitionName) {
    int index = getSeparatorIndex(partitionName);
    return partitionName.substring(0, index);
  }

  public static int getPartitionIdFromPartitionName(String partitionName) {
    int index = getSeparatorIndex(partitionName);
    String partitionId = partitionName.substring(index + 1);

    return Integer.parseInt(partitionId);
  }

  /**
   * truncate() operation mandates that all opened Database be closed before
   * attempting truncation.
   * <p/>
   * This method throws an exception while truncation is happening to any
   * request attempting in parallel with store truncation.
   *
   * @return database object
   */
  protected Database getBdbDatabase() {
    if (isTruncating.get()) {
      throw new VeniceException("BDB database " + getBdbDatabaseName()
        + " is currently truncating cannot serve any request.");
    }
    return this.database;
  }

  /**
   * Reopens the BDB database after a successful truncate operation.
   */
  private void reopenBdbDatabase() {
    try {
      database = environment.openDatabase(null, getBdbDatabaseName(), database.getConfig());
    } catch (DatabaseException e) {
      // this.bdbEnvironmentStats.reportException(e);
      throw new VeniceException("Failed to reinitialize BDB database " + getBdbDatabaseName() + " after truncation.", e);
    }
  }
}
