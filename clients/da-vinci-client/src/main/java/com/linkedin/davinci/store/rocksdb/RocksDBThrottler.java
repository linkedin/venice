package com.linkedin.davinci.store.rocksdb;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDBException;


/**
 * Abstract class to throttle RocksDB operations
 */
public abstract class RocksDBThrottler {
  private static final Logger LOGGER = LogManager.getLogger(RocksDBThrottler.class);
  private final int allowedMaxOperationsInParallel;
  private int currentOngoingOperations = 0;
  private final Lock throttlerLock = new ReentrantLock();
  private final Condition hasBandwidth = throttlerLock.newCondition();

  private final String operation;

  public RocksDBThrottler(String operation, int allowedMaxOperationsInParallel) {
    if (allowedMaxOperationsInParallel <= 0) {
      throw new IllegalArgumentException(
          "Param: allowedMaxOperationsInParallel should be positive, but is: " + allowedMaxOperationsInParallel);
    }
    this.allowedMaxOperationsInParallel = allowedMaxOperationsInParallel;
    this.operation = operation;
  }

  protected interface RocksDBOperationRunnable<T> {
    T execute() throws RocksDBException;
  }

  protected <T> T throttledOperation(String dbPath, RocksDBOperationRunnable<T> rocksDBOperationRunnable)
      throws InterruptedException, RocksDBException {
    throttlerLock.lock();
    try {
      while (currentOngoingOperations >= allowedMaxOperationsInParallel) {
        LOGGER.info("RocksDB database {} is being throttled for db path: {}", operation, dbPath);
        hasBandwidth.await();
      }
      ++currentOngoingOperations;
    } finally {
      throttlerLock.unlock();
    }
    T result;
    try {
      LOGGER.info("Performing RocksDB database {} for db path {}", operation, dbPath);
      result = rocksDBOperationRunnable.execute();
    } catch (RocksDBException e) {
      throw e;
    } finally {
      throttlerLock.lock();
      try {
        --currentOngoingOperations;
        hasBandwidth.signal();
      } finally {
        throttlerLock.unlock();
      }
    }
    return result;
  }
}
