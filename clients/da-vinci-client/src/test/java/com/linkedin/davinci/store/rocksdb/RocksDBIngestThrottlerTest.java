package com.linkedin.davinci.store.rocksdb;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_DB_INGEST_OPERATION_THROTTLE_DEFAULT;

import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDBException;
import org.testng.annotations.Test;


public class RocksDBIngestThrottlerTest {
  private static class TestSupplier implements RocksDBIngestThrottler.RocksDBSupplier {
    private static final Logger LOGGER = LogManager.getLogger(TestSupplier.class);
    private final int threadId;
    private final CountDownLatch latch;
    private final int waitTimeMs;

    public TestSupplier(int threadId, CountDownLatch latch, int waitTimeMs) {
      this.threadId = threadId;
      this.latch = latch;
      this.waitTimeMs = waitTimeMs;
    }

    @Override
    public void execute() throws RocksDBException {
      Utils.sleep(waitTimeMs);
      LOGGER.info(System.currentTimeMillis() + ", Get function invoked in thread: " + threadId);
      if (latch != null) {
        latch.countDown();
      }
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Param: allowedMaxIngestOperationsInParallel should be positive, but.*")
  public void testIngestThrottleBadInput() {
    new RocksDBIngestThrottler(0);
  }

  /**
   * Timeout passed to shutdownExecutor is more than the time it would require to finish all tasks
   * resulting in no tasks getting interrupted: Also Verified using a CountDownLatch
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testIngestThrottle() throws InterruptedException {
    int numTasks = 100;
    int waitTimeMS = 100;
    int bufferWaitMS = 10 * waitTimeMS;
    ExecutorService executorService = Executors.newFixedThreadPool(numTasks);
    // Create a CountDownLatch with the number of tasks
    CountDownLatch latch = new CountDownLatch(numTasks);

    try {
      RocksDBIngestThrottler throttler = new RocksDBIngestThrottler(ROCKSDB_DB_INGEST_OPERATION_THROTTLE_DEFAULT);
      for (int i = 0; i < numTasks; ++i) {
        final int threadId = i;
        executorService.submit(() -> {
          try {
            throttler.throttledIngest("/test_" + threadId, new TestSupplier(threadId, latch, waitTimeMS));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }
    } finally {
      TestUtils.shutdownExecutor(executorService, (numTasks * waitTimeMS) + bufferWaitMS, TimeUnit.MILLISECONDS);
      latch.await();
    }
  }

  /**
   * Timeout passed to shutdownExecutor is less than the time it would require to finish all tasks
   * resulting in some tasks getting interrupted.
   */
  @Test
  public void testIngestThrottleLesstime() throws InterruptedException {
    int numTasks = 2;
    int waitTimeMS = 100;
    ExecutorService executorService = Executors.newFixedThreadPool(numTasks);

    try {
      RocksDBIngestThrottler throttler = new RocksDBIngestThrottler(ROCKSDB_DB_INGEST_OPERATION_THROTTLE_DEFAULT);
      for (int i = 0; i < numTasks; ++i) {
        final int threadId = i;
        executorService.submit(() -> {
          try {
            throttler.throttledIngest("/test_" + threadId, new TestSupplier(threadId, null, waitTimeMS));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }
    } finally {
      TestUtils.shutdownExecutor(executorService, 1, TimeUnit.MILLISECONDS, false);
    }
  }
}
