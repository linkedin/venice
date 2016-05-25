package com.linkedin.venice.store.bdb;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.store.AbstractStoragePartition;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.partition.iterators.AbstractCloseablePartitionEntriesIterator;
import com.linkedin.venice.utils.partition.iterators.CloseablePartitionKeysIterator;
import com.sleepycat.je.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.concurrent.atomic.AtomicBoolean;

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

  private final String storeName;
  private final Environment environment;
  private Database database;

  private final DatabaseConfig databaseConfig;
  private final LockMode readLockMode;
  private final boolean minimizeScanImpact;

  private final AtomicBoolean isOpen;
  private final AtomicBoolean isTruncating = new AtomicBoolean(false);

  public BdbStoragePartition(String storeName, Integer partitionId, Environment environment, BdbServerConfig bdbServerConfig) {
    super(partitionId);

    this.storeName = Utils.notNull(storeName);
    this.environment = Utils.notNull(environment);

    this.databaseConfig = new DatabaseConfig();
    this.databaseConfig.setAllowCreate(true);
    this.databaseConfig.setSortedDuplicates(false);
    this.databaseConfig.setNodeMaxEntries(bdbServerConfig.getBdbBtreeFanout());
    this.databaseConfig.setTransactional(true);

    this.database = this.environment.openDatabase(null, getBdbDatabaseName(), databaseConfig);

    BdbRuntimeConfig bdbRuntimeConfig = new BdbRuntimeConfig(bdbServerConfig);
    this.readLockMode = bdbRuntimeConfig.getLockMode();
    this.minimizeScanImpact = bdbRuntimeConfig.getMinimizeScanImpact();

    this.isOpen = new AtomicBoolean(true);
  }

  public void put(byte[] key, byte[] value) throws VeniceException {

    boolean succeeded = false;
    long startTimeNs = -1;

    if (logger.isTraceEnabled())
      startTimeNs = System.nanoTime();

    DatabaseEntry keyEntry = new DatabaseEntry(key);
    DatabaseEntry valueEntry = new DatabaseEntry(value);

    Transaction transaction = null;

    try {
      transaction = environment.beginTransaction(null, null);

      /* TODO: decide where to add code for conflict resolution
         if it is in propagation layer, then remove this comment;
         otherwise read entry here and modify it */
      OperationStatus status = getBdbDatabase().put(transaction, keyEntry, valueEntry);

      if (status != OperationStatus.SUCCESS) {
        throw new VeniceException("Put operation failed with status: " + status + " Store " + getBdbDatabaseName() );
      }

      succeeded = true;

    } catch (DatabaseException e) {
      //this.bdbEnvironmentStats.reportException(e);
      logger.error("Error in put for BDB database " + getBdbDatabaseName(), e);
      throw new VeniceException(e);
    } finally {
      commitOrAbort(succeeded, transaction);
      if (logger.isTraceEnabled()) {
        logger.trace(succeeded ? "Successfully completed" : "Failed to complete"
          + " PUT (" + getBdbDatabaseName() + ") to key " + key + " (keyRef: "
          + System.identityHashCode(key) + " value " + value + " in "
          + (System.nanoTime() - startTimeNs) + " ns at "
          + System.currentTimeMillis());
      }
    }
  }


  public byte[] get(byte[] key) throws VeniceException {

    boolean succeeded = true;
    long startTimeNs = -1;

    DatabaseEntry keyEntry = new DatabaseEntry(key);
    DatabaseEntry valueEntry = new DatabaseEntry();


    if (logger.isTraceEnabled()) {
      startTimeNs = System.nanoTime();
    }

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
      succeeded = false;
      //this.bdbEnvironmentStats.reportException(e);
      logger.error(e);
      throw new VeniceException(e);
    } finally {
      if (logger.isTraceEnabled()) {
        logger.trace(succeeded ? "Successfully completed" : "Failed to complete"
          + " GET (" + getBdbDatabaseName() + ") from key " + key + " (keyRef: "
          + System.identityHashCode(key) + ") in "
          + (System.nanoTime() - startTimeNs) + " ns at "
          + System.currentTimeMillis());
      }
    }
  }

  public void delete(byte[] key) {

    boolean succeeded = false;
    long startTimeNs = -1;

    if (logger.isTraceEnabled()) {
      startTimeNs = System.nanoTime();
    }

    Transaction transaction = null;
    try {
      transaction = this.environment.beginTransaction(null, null);
      DatabaseEntry keyEntry = new DatabaseEntry(key);

      OperationStatus status = getBdbDatabase().delete(transaction, keyEntry);
      succeeded = true;
    } catch (DatabaseException e) {
      //this.bdbEnvironmentStats.reportException(e);
      logger.error(e);
      throw new VeniceException(e);
    } finally {
      attemptCommit(transaction);
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

  @Override
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

  @Override
  public CloseablePartitionKeysIterator partitionKeys() {
    return new CloseablePartitionKeysIterator(partitionEntries());
  }


  interface BDBOperation {
    void perform(Transaction x);
  }

  private synchronized  void performOperation(String operationType, BDBOperation operation) {
    Transaction transaction = null;
    boolean succeeded = false;

    try {
      transaction = this.environment.beginTransaction(null, null);

      // close current bdbDatabase first
      database.close();
      // truncate the database
      operation.perform(transaction);
      succeeded = true;
    } catch (DatabaseException e) {
      String errorMessage = "Failed to " + operationType + " BDB database " + getBdbDatabaseName();
      logger.error(errorMessage, e);
      throw new VeniceException(errorMessage , e);

    } finally {
      commitOrAbort(succeeded, transaction);
    }

  }

  /**
   * Drop the BDB database, when it is not required anymore.
   */
  @Override
  public synchronized void drop() {
    BDBOperation dropDB =(tx) -> environment.removeDatabase(tx, getBdbDatabaseName());
    performOperation("DROP" , dropDB );
  }

  /**
   * Truncate all the entries in the BDB database.
   */
  @Override
  public synchronized void truncate() {
    BDBOperation truncateDB = (tx) -> environment.truncateDatabase(tx, getBdbDatabaseName(), false);
    performOperation("TRUNCATE" , truncateDB );

    //After truncation re-open the BDB database for Read.
    reopenBdbDatabase();
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
    return storeName + "-" + partitionId;
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

  private void attemptAbort(Transaction transaction) {
    try {
      if (transaction != null)
        transaction.abort();
    } catch (DatabaseException e) {
      //this.bdbEnvironmentStats.reportException(e);
      logger.error("Abort failed!", e);
    }
  }

  private void attemptCommit(Transaction transaction) {
    try {
      if (transaction != null)
        transaction.commit();
    } catch (DatabaseException e) {
      //this.bdbEnvironmentStats.reportException(e);
      logger.error("Transaction commit failed!", e);
      attemptAbort(transaction);
      throw new VeniceException(e);
    }
  }

  private void commitOrAbort(boolean succeeded, Transaction transaction) {
    try {
      if (succeeded) {
        attemptCommit(transaction);
      } else {
        attemptAbort(transaction);
      }
    } catch (Exception e) {
      logger.error(e);
    }
  }
}
