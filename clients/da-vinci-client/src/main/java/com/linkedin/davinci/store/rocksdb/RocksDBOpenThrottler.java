package com.linkedin.davinci.store.rocksdb;

import java.util.List;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;


/**
 * check {@link RocksDBServerConfig#ROCKSDB_DB_OPEN_OPERATION_THROTTLE} to find more details.
 */
public class RocksDBOpenThrottler extends RocksDBThrottler {
  public RocksDBOpenThrottler(int allowedMaxOpenOperationsInParallel) {
    super("open", allowedMaxOpenOperationsInParallel);
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
    return throttledOperation(
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
    return throttledOperation(
        dbPath,
        () -> RocksDB.open(new DBOptions(options), dbPath, columnFamilyDescriptors, columnFamilyHandles));
  }
}
