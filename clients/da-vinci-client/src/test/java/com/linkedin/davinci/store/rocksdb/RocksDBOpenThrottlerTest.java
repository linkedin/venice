package com.linkedin.davinci.store.rocksdb;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_DB_OPEN_OPERATION_THROTTLE_DEFAULT;

import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.testng.annotations.Test;


public class RocksDBOpenThrottlerTest {
  private static class TestSupplier implements RocksDBOpenThrottler.RocksDBSupplier {
    private static final Logger LOGGER = LogManager.getLogger(TestSupplier.class);
    private final int threadId;

    public TestSupplier(int threadId) {
      this.threadId = threadId;
    }

    @Override
    public RocksDB get() throws RocksDBException {
      Utils.sleep(100);
      LOGGER.info(System.currentTimeMillis() + ", Get function invoked in thread: " + threadId);
      return null;
    }
  }

  @Test
  public void testOpenThrottle() throws InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(100);
    try {
      RocksDBOpenThrottler throttler = new RocksDBOpenThrottler(ROCKSDB_DB_OPEN_OPERATION_THROTTLE_DEFAULT);
      for (int i = 0; i < 100; ++i) {
        final int threadId = i;
        executorService.submit(() -> throttler.throttledOpen("/test_" + threadId, new TestSupplier(threadId)));
      }
    } finally {
      TestUtils.shutdownExecutor(executorService, 1, TimeUnit.MINUTES);
    }
  }
}
