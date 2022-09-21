package com.linkedin.davinci.store.rocksdb;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;


/**
 * This class is used to throttle RocksDB operations.
 * So far, it only supports throttling open operations.
 * check {@link RocksDBServerConfig#ROCKSDB_DB_OPEN_OPERATION_THROTTLE} to find more details.
 */
public class RocksDBThrottler {
  private static final Logger LOGGER = LogManager.getLogger(RocksDBThrottler.class);

  private final int allowedMaxOpenOperationsInParallel;
  private int currentOngoingOpenOperations = 0;

  private final Lock throttlerLock = new ReentrantLock();
  private final Condition hasBandwidth = throttlerLock.newCondition();

  public RocksDBThrottler(int allowedMaxOpenOperationsInParallel) {
    if (allowedMaxOpenOperationsInParallel <= 0) {
      throw new IllegalArgumentException(
          "Param: allowedMaxOpenOperationsInParallel should be positive, but is: "
              + allowedMaxOpenOperationsInParallel);
    }
    this.allowedMaxOpenOperationsInParallel = allowedMaxOpenOperationsInParallel;
  }

  protected interface RocksDBSupplier {
    RocksDB get() throws RocksDBException;
  }

  protected RocksDB throttledOpen(String dbPath, RocksDBSupplier rocksDBSupplier)
      throws InterruptedException, RocksDBException {
    throttlerLock.lock();
    try {
      while (currentOngoingOpenOperations >= allowedMaxOpenOperationsInParallel) {
        LOGGER.info("RocksDB database open operation is being throttled for db path: {}", dbPath);
        hasBandwidth.await();
      }
      ++currentOngoingOpenOperations;
    } finally {
      throttlerLock.unlock();
    }
    RocksDB db;
    try {
      LOGGER.info("Opening RocksDB database: {}", dbPath);
      db = rocksDBSupplier.get();
    } catch (RocksDBException e) {
      throw e;
    } finally {
      throttlerLock.lock();
      try {
        --currentOngoingOpenOperations;
        hasBandwidth.signal();
      } finally {
        throttlerLock.unlock();
      }
    }
    return db;
  }

  /**
   * Open RocksDB in read-only mode with provided column family descriptors and handlers.
   */
  public RocksDB openReadOnly(
      Options options,
      String dbPath,
      List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      List<ColumnFamilyHandle> columnFamilyHandles) throws RocksDBException, InterruptedException {
    columnFamilyHandles.clear(); // Make sure we pass in a clean column family handle list. RocksDB JNI only calls add
                                 // to insert each handle.
    return throttledOpen(
        dbPath,
        () -> RocksDB.openReadOnly(new DBOptions(options), dbPath, columnFamilyDescriptors, columnFamilyHandles));
  }

  /**
   * Open RocksDB in read-write mode with provided column family descriptors and handlers.
   */
  public RocksDB open(
      Options options,
      String dbPath,
      List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      List<ColumnFamilyHandle> columnFamilyHandles) throws RocksDBException, InterruptedException {
    columnFamilyHandles.clear(); // Make sure we pass in a clean column family handle list. RocksDB JNI only calls add
                                 // to insert each handle.
    return throttledOpen(
        dbPath,
        () -> RocksDB.open(new DBOptions(options), dbPath, columnFamilyDescriptors, columnFamilyHandles));
  }
}
