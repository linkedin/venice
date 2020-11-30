package com.linkedin.davinci.store.rocksdb;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;


/**
 * This class is used to throttle RocksDB operations.
 * So far, it only supports throttling open operations.
 * check {@link RocksDBServerConfig#ROCKSDB_DB_OPEN_OPERATION_THROTTLE} to find more details.
 */
public class RocksDBThrottler {
  private static final Logger LOGGER = Logger.getLogger(RocksDBThrottler.class);

  private final int allowedMaxOpenOperationsInParallel;
  private int currentOngoingOpenOperations = 0;

  private final Lock throttlerLock = new ReentrantLock();
  private final Condition hasBandwidth = throttlerLock.newCondition();

  public RocksDBThrottler(int allowedMaxOpenOperationsInParallel) {
    if (allowedMaxOpenOperationsInParallel <= 0) {
      throw new IllegalArgumentException("Param: allowedMaxOpenOperationsInParallel should be positive, but is: "
          + allowedMaxOpenOperationsInParallel);
    }
    this.allowedMaxOpenOperationsInParallel = allowedMaxOpenOperationsInParallel;
  }

  protected interface RocksDBSupplier {
    RocksDB get() throws RocksDBException;
  }

  protected RocksDB throttledOpen(String dbPath, RocksDBSupplier rocksDBSupplier) throws InterruptedException, RocksDBException {
    throttlerLock.lock();
    try {
      while (currentOngoingOpenOperations >= allowedMaxOpenOperationsInParallel) {
        LOGGER.info("RocksDB database open operation is being throttled for db path: " + dbPath);
        hasBandwidth.await();
      }
      ++currentOngoingOpenOperations;
    } finally {
      throttlerLock.unlock();
    }
    RocksDB db;
    try {
      LOGGER.info("Opening RocksDB database: " + dbPath);
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
   * Open RocksDB in read-only mode.
   */
  public RocksDB openReadOnly(Options options, String dbPath) throws InterruptedException, RocksDBException {
    return throttledOpen(dbPath, () -> RocksDB.openReadOnly(options, dbPath));
  }

  /**
   * Open RocksDB in read-write mode.
   */
  public RocksDB open(Options options, String dbPath) throws RocksDBException, InterruptedException {
    return throttledOpen(dbPath, () -> RocksDB.open(options, dbPath));
  }
}
