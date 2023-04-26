package com.linkedin.davinci.store.rocksdb;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDBException;


/**
 * This class is a copy of {@link RocksDBOpenThrottler}
 * check {@link RocksDBServerConfig#ROCKSDB_DB_INGEST_OPERATION_THROTTLE} to find more details.
 */
public class RocksDBIngestThrottler {
  private static final Logger LOGGER = LogManager.getLogger(RocksDBIngestThrottler.class);

  private final int allowedMaxIngestOperationsInParallel;
  private int currentOngoingIngestOperations = 0;

  private final Lock throttlerLock = new ReentrantLock();
  private final Condition hasBandwidth = throttlerLock.newCondition();

  public RocksDBIngestThrottler(int allowedMaxIngestOperationsInParallel) {
    if (allowedMaxIngestOperationsInParallel <= 0) {
      throw new IllegalArgumentException(
          "Param: allowedMaxIngestOperationsInParallel should be positive, but is: "
              + allowedMaxIngestOperationsInParallel);
    }
    this.allowedMaxIngestOperationsInParallel = allowedMaxIngestOperationsInParallel;
  }

  protected interface RocksDBSupplier {
    void execute() throws RocksDBException;
  }

  protected void throttledIngest(String dbPath, RocksDBSupplier rocksDBSupplier)
      throws RocksDBException, InterruptedException {
    throttlerLock.lock();
    try {
      while (currentOngoingIngestOperations >= allowedMaxIngestOperationsInParallel) {
        LOGGER.info("RocksDB database ingest operation is being throttled for db path: {}", dbPath);
        hasBandwidth.await();
      }
      ++currentOngoingIngestOperations;
    } finally {
      throttlerLock.unlock();
    }
    try {
      LOGGER.info("Ingesting RocksDB database: {}", dbPath);
      rocksDBSupplier.execute();
    } catch (RocksDBException e) {
      throw e;
    } finally {
      throttlerLock.lock();
      try {
        --currentOngoingIngestOperations;
        hasBandwidth.signal();
      } finally {
        throttlerLock.unlock();
      }
    }
  }
}
