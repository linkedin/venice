package com.linkedin.davinci.store.rocksdb;

import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RocksDBThrottlerTest {
  private static final Logger LOGGER = LogManager.getLogger(RocksDBThrottlerTest.class);

  /**
   * Liveness: Testing whether all the tasks invoking the throttlers gets executed.
   * Correctness: Testing whether only the allowedMaxOperationsInParallel are executed at any time
   *
   * Create a throttler with limit 1
   * Submit a task to thread pool that will call throttledOperation with latch.await() as the payload.
   * Submit another task to thread pool that will call throttledOperation with empty lambda as the payload.
   * Assert that the second task is not complete.
   * Release the latch.
   * Assert that the first task is complete.
   * Assert that the second task is complete.
   */

  @Test(timeOut = 20 * Time.MS_PER_SECOND)
  public void testThrottleWith2Tasks() throws InterruptedException {
    int allowedMaxOperationsInParallel = 1;
    /**
     * Number of threads is set up to more than the number of tasks to have enough threads
     * to have the ability to schedule all the tasks at the same time if needed and then
     * test the throttler's functionality.
     */
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    CountDownLatch taskWaitLatch = new CountDownLatch(1);
    CountDownLatch task1StartStatusLatch = new CountDownLatch(1);
    CountDownLatch task2StartStatusLatch = new CountDownLatch(1);

    try {
      RocksDBIngestThrottler throttler = new RocksDBIngestThrottler(allowedMaxOperationsInParallel);

      // submit task 1: once started will finish the task1StartStatusLatch such that
      // we know it's scheduled before sending in the second task
      Future task1Future = executorService.submit(() -> {
        try {
          throttler.throttledOperation("/test_1", () -> {
            task1StartStatusLatch.countDown();
            taskWaitLatch.await();
            return null;
          });
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      // make sure task 1 started
      task1StartStatusLatch.await();

      // submit task 2
      Future task2Future = executorService.submit(() -> {
        try {
          throttler.throttledOperation("/test_2", () -> {
            task2StartStatusLatch.countDown();
            return null;
          });
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      // make sure task 2 didn't start: Test will work with this as well,
      // but we aren't really giving it time to correctly verify that task2
      // has not yet started if we don't wait for some time.
      task2StartStatusLatch.await(5, TimeUnit.SECONDS);
      Assert.assertFalse(task1Future.isDone());
      Assert.assertFalse(task2Future.isDone());

      // Finish the task 1
      taskWaitLatch.countDown();

      // Now the task 1 should finish and task 2 should get executed and finish as well.
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        Assert.assertTrue(task1Future.isDone());
        Assert.assertTrue(task2Future.isDone());
      });
    } finally {
      TestUtils.shutdownExecutorNow(executorService);
    }
  }

  /**
   * similar to above, but with more tasks
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testThrottleWith10Tasks() throws InterruptedException {
    int allowedMaxOperationsInParallel = 5;
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    List<CountDownLatch> taskWaitLatches = new ArrayList<>();
    List<CountDownLatch> taskStartStatusLatches = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      taskWaitLatches.add(new CountDownLatch(1));
      taskStartStatusLatches.add(new CountDownLatch(1));
    }

    try {
      RocksDBIngestThrottler throttler = new RocksDBIngestThrottler(allowedMaxOperationsInParallel);

      List<Future> taskFutures = new ArrayList<>();
      for (int i = 0; i < 5; ++i) {
        // submit tasks 0 to 4 which will be starting to execute but will be waiting on the
        int finalI = i;
        taskFutures.add(executorService.submit(() -> {
          try {
            throttler.throttledOperation("/test_" + finalI, () -> {
              taskStartStatusLatches.get(finalI).countDown();
              taskWaitLatches.get(finalI).await();
              return null;
            });
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }));
      }

      // make sure the tasks 0-4 started
      for (int i = 0; i < 5; i++) {
        taskStartStatusLatches.get(i).await();
      }

      // verify that that none of the task 0 to 4 is completed
      taskFutures.stream().forEach(future -> {
        Assert.assertFalse(future.isDone());
      });

      // submit task 5
      taskFutures.add(executorService.submit(() -> {
        try {
          throttler.throttledOperation("/test_5", () -> {
            taskStartStatusLatches.get(5).countDown();
            taskWaitLatches.get(5).await();
            return null;
          });
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));

      // wait for sometime to see task 5 didn't start as the throttler
      // can only run 5 tasks at a time (0-4 which are already scheduled)
      taskStartStatusLatches.get(5).await(5, TimeUnit.SECONDS);

      // none of the tasks 0-4 will be completed and task 5 won't be even scheduled
      taskFutures.stream().forEach(future -> {
        Assert.assertFalse(future.isDone());
      });

      List<Integer> completedFutureidx = new ArrayList<>();

      LOGGER.info("latch(0) countDown");
      taskWaitLatches.get(0).countDown();
      // task 0 should be completed due to the latch countDown
      completedFutureidx.add(0);
      // task 5 should be scheduled as it's the only task next in line
      taskStartStatusLatches.get(5).await();
      // complete task 5 as well
      LOGGER.info("latch(5) countDown");
      taskWaitLatches.get(5).countDown();
      completedFutureidx.add(5);

      // test whether 0 and 5 are completed and everything else is not completed
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        Assert.assertTrue(taskFutures.get(0).isDone());
        Assert.assertTrue(taskFutures.get(5).isDone());
        taskFutures.stream()
            .filter(future -> !completedFutureidx.contains(taskFutures.indexOf(future)))
            .forEach(future -> {
              Assert.assertFalse(future.isDone());
            });
      });

      // submit task 6
      taskFutures.add(executorService.submit(() -> {
        try {
          throttler.throttledOperation("/test_6", () -> {
            taskStartStatusLatches.get(6).countDown();
            taskWaitLatches.get(6).await();
            return null;
          });
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));

      // wait for sometime to see task 6 didn't start
      taskStartStatusLatches.get(6).await(5, TimeUnit.SECONDS);

      LOGGER.info("latch(1) countDown");
      taskWaitLatches.get(1).countDown();
      completedFutureidx.add(1);
      // task 6 should be scheduled as it's the only task next in line
      taskStartStatusLatches.get(6).await();
      // complete task 6 as well
      LOGGER.info("latch(6) countDown");
      taskWaitLatches.get(6).countDown();
      completedFutureidx.add(6);

      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        Assert.assertTrue(taskFutures.get(1).isDone());
        Assert.assertTrue(taskFutures.get(6).isDone());
        taskFutures.stream()
            .filter(future -> !completedFutureidx.contains(taskFutures.indexOf(future)))
            .forEach(future -> {
              Assert.assertFalse(future.isDone());
            });
      });

      // submit task 7
      taskFutures.add(executorService.submit(() -> {
        try {
          throttler.throttledOperation("/test_7", () -> {
            taskStartStatusLatches.get(7).countDown();
            taskWaitLatches.get(7).await();
            return null;
          });
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));

      // wait for sometime to see task 7 didn't start
      taskStartStatusLatches.get(7).await(5, TimeUnit.SECONDS);

      LOGGER.info("latch(2) countDown");
      taskWaitLatches.get(2).countDown();
      completedFutureidx.add(2);
      // task 7 should be scheduled as it's the only task next in line
      taskStartStatusLatches.get(7).await();
      // complete task 7 as well
      LOGGER.info("latch(7) countDown");
      taskWaitLatches.get(7).countDown();
      completedFutureidx.add(7);

      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        Assert.assertTrue(taskFutures.get(2).isDone());
        Assert.assertTrue(taskFutures.get(7).isDone());
        taskFutures.stream()
            .filter(future -> !completedFutureidx.contains(taskFutures.indexOf(future)))
            .forEach(future -> {
              Assert.assertFalse(future.isDone());
            });
      });

      // submit task 8
      taskFutures.add(executorService.submit(() -> {
        try {
          throttler.throttledOperation("/test_8", () -> {
            taskStartStatusLatches.get(8).countDown();
            taskWaitLatches.get(8).await();
            return null;
          });
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));

      // wait for sometime to see task 8 didn't start
      taskStartStatusLatches.get(8).await(5, TimeUnit.SECONDS);

      LOGGER.info("latch(3) countDown");
      taskWaitLatches.get(3).countDown();
      completedFutureidx.add(3);
      // task 8 should be scheduled as it's the only task next in line
      taskStartStatusLatches.get(8).await();
      // complete task 8 as well
      LOGGER.info("latch(8) countDown");
      taskWaitLatches.get(8).countDown();
      completedFutureidx.add(8);

      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        Assert.assertTrue(taskFutures.get(3).isDone());
        Assert.assertTrue(taskFutures.get(8).isDone());
        taskFutures.stream()
            .filter(future -> !completedFutureidx.contains(taskFutures.indexOf(future)))
            .forEach(future -> {
              Assert.assertFalse(future.isDone());
            });
      });

      // submit task 9
      taskFutures.add(executorService.submit(() -> {
        try {
          throttler.throttledOperation("/test_9", () -> {
            taskStartStatusLatches.get(9).countDown();
            taskWaitLatches.get(9).await();
            return null;
          });
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));

      // wait for sometime to see task 9 didn't start
      taskStartStatusLatches.get(9).await(5, TimeUnit.SECONDS);

      LOGGER.info("latch(4) countDown");
      taskWaitLatches.get(4).countDown();
      completedFutureidx.add(4);
      // task 9 should be scheduled as it's the only task next in line
      taskStartStatusLatches.get(9).await();
      // complete task 9 as well
      LOGGER.info("latch(9) countDown");
      taskWaitLatches.get(9).countDown();
      completedFutureidx.add(9);

      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        Assert.assertTrue(taskFutures.get(4).isDone());
        Assert.assertTrue(taskFutures.get(9).isDone());
        Assert.assertEquals(completedFutureidx.size(), 10);
      });
    } finally {
      TestUtils.shutdownExecutorNow(executorService);
    }
  }
}
