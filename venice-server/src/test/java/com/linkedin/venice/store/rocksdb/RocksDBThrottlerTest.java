package com.linkedin.venice.store.rocksdb;

import com.linkedin.venice.utils.Utils;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.testng.annotations.Test;


public class RocksDBThrottlerTest {

  private static class TestSupplier implements RocksDBThrottler.RocksDBSupplier {
    private final int threadId;

    public TestSupplier(int threadId) {
      this.threadId = threadId;
    }

    @Override
    public RocksDB get() throws RocksDBException {
      Utils.sleep(100);
      System.out.println(System.currentTimeMillis() + ", Get function invoked in thread: " + threadId);
      return null;
    }
  }


  @Test
  public void testThrottle() throws RocksDBException, InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(100);
    RocksDBThrottler throttler = new RocksDBThrottler(5);

    for (int i = 0; i < 100; ++i) {
      final int threadId = i;
      executorService.submit(() -> throttler.throttledOpen("/test", new TestSupplier(threadId)));
    }
    executorService.shutdown();

    executorService.awaitTermination(100, TimeUnit.SECONDS);
  }
}
