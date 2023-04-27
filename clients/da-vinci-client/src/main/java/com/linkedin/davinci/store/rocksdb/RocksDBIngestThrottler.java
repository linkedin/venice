package com.linkedin.davinci.store.rocksdb;

import org.rocksdb.RocksDBException;


/**
 * check {@link RocksDBServerConfig#ROCKSDB_DB_INGEST_OPERATION_THROTTLE} to find more details.
 */
public class RocksDBIngestThrottler extends RocksDBThrottler {
  public RocksDBIngestThrottler(int allowedMaxIngestOperationsInParallel) {
    super("ingest", allowedMaxIngestOperationsInParallel);
  }

  public void throttledIngest(String dbPath, RocksDBOperationRunnable<Void> rocksDBOperationRunnable)
      throws RocksDBException, InterruptedException {
    throttledOperation(dbPath, rocksDBOperationRunnable);
  }
}
