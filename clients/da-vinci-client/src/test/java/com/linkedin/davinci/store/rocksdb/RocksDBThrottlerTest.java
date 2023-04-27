package com.linkedin.davinci.store.rocksdb;

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
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Number of threads is set up to be equal to Number of tasks to have enough threads
 * to have the ability to schedule all the tasks at the same time if needed and then
 * test the throttler's functionality.
 */
public class RocksDBThrottlerTest {
  private static class TestOperationRunnable implements RocksDBThrottler.RocksDBOperationRunnable<Void> {
    private static final Logger LOGGER = LogManager.getLogger(TestOperationRunnable.class);
    private final int threadId;
    private final CountDownLatch latch;
    private final int waitTimeMs;

    public TestOperationRunnable(int threadId, CountDownLatch latch, int waitTimeMs) {
      this.threadId = threadId;
      this.latch = latch;
      this.waitTimeMs = waitTimeMs;
    }

    @Override
    public Void execute() throws RocksDBException {
      LOGGER.info(System.currentTimeMillis() + ", Get function invoked in thread: " + threadId);
      if (latch != null) {
        latch.countDown();
      }
      Utils.sleep(waitTimeMs);
      return null;
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Param: allowedMaxOperationsInParallel should be positive, but.*")
  public void testThrottleBadInput() {
    new RocksDBIngestThrottler(0);
  }

  /**
   * Liveness: Testing whether all the tasks invoking the throttlers gets executed.
   * - Timeout passed to shutdownExecutor is more than the time it would require to finish all tasks
   *   resulting in no tasks getting interrupted.
   * - allowedMaxOperationsInParallel is set to 1, number of tasks is set to 5 and each tasks will
   *   sleep for 1 sec, so in 5 seconds all the tasks should have finished executing.
   * - Verified using CountDownLatch to account for the number of tasks getting executed
   */
  @Test(timeOut = 10 * Time.MS_PER_SECOND)
  public void testThrottleLiveness() throws InterruptedException {
    int numTasks = 5;
    int waitTimeMS = 1 * Time.MS_PER_SECOND; // 1 sec
    int bufferWaitMS = 10 * waitTimeMS; // extra buffer time
    ExecutorService executorService = Executors.newFixedThreadPool(numTasks);
    // Create a CountDownLatch with the number of tasks
    CountDownLatch latch = new CountDownLatch(numTasks);

    try {
      RocksDBIngestThrottler throttler = new RocksDBIngestThrottler(1);
      for (int i = 0; i < numTasks; ++i) {
        final int threadId = i;
        executorService.submit(() -> {
          try {
            throttler.throttledOperation("/test_" + threadId, new TestOperationRunnable(threadId, latch, waitTimeMS));
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
   * Correctness: Testing whether only the allowedMaxOperationsInParallel are executed at any time.
   * - allowedMaxOperationsInParallel is set to 5, number of tasks is set to 10 and each tasks will
   *   sleep for 10 sec, so all the tasks should have finished executing in 20 seconds, but that is not
   *   being checked here.
   * - Once the first 5 tasks starts getting executed (ie starts sleeping), we wait for 1 second and
   *   verify whether CountDownLatch is equal to 5 which is the remaining tasks after the first batch
   *   of 5 tasks getting executed, to verify whether the throttler only executes a maximum of this many
   *   number of tasks in parallel.
   * - again check after 7 seconds (total 8 seconds) and CountDownLatch should be 5 even now as the
   *   first set of tasks are still sleeping
   * - again check after 3 seconds (total 11 seconds) and CountDownLatch should be 0 as the second set
   *   of tasks should have started executing as the sleep time inside task is only 10 seconds.
   */
  @Test(timeOut = 25 * Time.MS_PER_SECOND)
  public void testThrottleCorrectness() throws InterruptedException {
    int numTasks = 10;
    int waitTimeMS = 10 * Time.MS_PER_SECOND; // 10 secs
    int allowedMaxOperationsInParallel = 5;
    ExecutorService executorService = Executors.newFixedThreadPool(numTasks);
    // Create a CountDownLatch with the number of tasks
    CountDownLatch latch = new CountDownLatch(numTasks);

    try {
      RocksDBIngestThrottler throttler = new RocksDBIngestThrottler(allowedMaxOperationsInParallel);
      for (int i = 0; i < numTasks; ++i) {
        final int threadId = i;
        executorService.submit(() -> {
          try {
            throttler.throttledOperation("/test_" + threadId, new TestOperationRunnable(threadId, latch, waitTimeMS));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }
    } finally {
      Thread.sleep(1 * Time.MS_PER_SECOND);
      // First 5 tasks should be currently executing, so 5 remaining tasks
      Assert.assertEquals(latch.getCount(), allowedMaxOperationsInParallel);
      Thread.sleep(7 * Time.MS_PER_SECOND);
      // First 5 tasks should still be currently executing, so 5 remaining tasks
      Assert.assertEquals(latch.getCount(), allowedMaxOperationsInParallel);
      Thread.sleep(3 * Time.MS_PER_SECOND);
      // Next 5 tasks should have started executing as its more than 10 seconds, so 0 remaining tasks
      Assert.assertEquals(latch.getCount(), 0);
      TestUtils.shutdownExecutorNow(executorService);
    }
  }
}
