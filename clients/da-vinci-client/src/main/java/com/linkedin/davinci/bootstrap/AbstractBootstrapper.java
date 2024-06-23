package com.linkedin.davinci.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;


public abstract class AbstractBootstrapper implements RocksDbBootstrapper {
  static {
    RocksDB.loadLibrary();
  }

  private static final Logger LOGGER = LogManager.getLogger(AbstractBootstrapper.class);

  protected void verifyBootstrap(String newDbDir) throws RocksDBException {
    // Open the RocksDB instance & check for the first entry
    Options options = new Options().setCreateIfMissing(false);
    try (RocksDB db = RocksDB.open(options, newDbDir)) {
      if (db != null) {
        try (RocksIterator iterator = db.newIterator()) {
          iterator.seekToFirst();
          if (iterator.isValid()) {
            LOGGER.info("Successfully bootstrapped RocksDB.");
          } else {
            LOGGER.warn("RocksDB instance is empty.");
          }
        }
      } else {
        LOGGER.warn("RocksDB instance could not be opened. Cannot verify if there are entries in DB after bootstrap.");
      }
    }
  }

  protected abstract void bootstrapFromBlobs(String storeName, int versionNumber, int partitionId) throws Exception;

  protected abstract void bootstrapFromKafka(int partitionId);
}
