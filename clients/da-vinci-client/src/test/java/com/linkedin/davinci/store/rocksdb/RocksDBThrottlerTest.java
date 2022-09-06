package com.linkedin.davinci.store.rocksdb;

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


public class RocksDBThrottlerTest {
  private static class TestSupplier implements RocksDBThrottler.RocksDBSupplier {
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
  public void testThrottle() throws InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(100);
    try {
      RocksDBThrottler throttler = new RocksDBThrottler(5);
      for (int i = 0; i < 100; ++i) {
        final int threadId = i;
        executorService.submit(() -> throttler.throttledOpen("/test", new TestSupplier(threadId)));
      }
    } finally {
      TestUtils.shutdownExecutor(executorService, 1, TimeUnit.MINUTES);
    }
  }
}
