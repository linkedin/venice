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
   * This tests for both liveness and correctness
   * Liveness: Testing whether all the tasks invoking the throttlers gets executed.
   * Correctness: Testing whether only the allowedMaxOperationsInParallel are executed at any time
   *
   * 1. Submit 5 tasks with 5 being throttler's allowedMaxOperationsInParallel
   * 2. Finish the tasks 1 by one while submitting a new task for every finished task
   * 3. Once the initial 5 tasks are finished, throttler will be running the new 5 tasks,
   *    Finish the new tasks as well.
   * 4. Verify all tasks are finished
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testThrottle() throws InterruptedException {
    int allowedMaxOperationsInParallel = 5;
    // start a big enough threadpool to have the ability to run all the tasks at a time
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    // tasks will wait on this latch which can be finished from the main thread to finish a task
    List<CountDownLatch> taskWaitLatches = new ArrayList<>();
    // main thread can wait on this latch to see if the tasks is started
    List<CountDownLatch> taskStartStatusLatches = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      taskWaitLatches.add(new CountDownLatch(1));
      taskStartStatusLatches.add(new CountDownLatch(1));
    }

    try {
      RocksDBIngestThrottler throttler = new RocksDBIngestThrottler(allowedMaxOperationsInParallel);

      List<Future> taskFutures = new ArrayList<>();

      // 1. submit the initial 5 tasks: 0 to 4 will start to execute as throttler allows
      // max 5 in parallel but will wait on taskWaitLatches
      for (int i = 0; i < 5; ++i) {
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

      // list to track the completed tasks
      List<Integer> completedFutureidx = new ArrayList<>();

      // 2. Finish the tasks 1 by one while submitting a new task for every finished task
      //
      // We have 5 tasks scheduled by the throttler so far which are not finished yet. Inside the loop:
      // 2.a: submit a new task, but it won't start as throttler is maxed already
      // 2.b: Finish one of the currently executing tasks to free up space for the new task scheduled in 2.a
      // 2.c: The new task created in 2.a will start executing now
      for (int i = 0; i < 5; i++) {
        int currentTaskToBeExecuted = i; // existing task to be finished
        int nextTaskToBeScheduled = i + 5; // new task to be submitted

        // submit the new task
        taskFutures.add(executorService.submit(() -> {
          try {
            throttler.throttledOperation("/test_" + nextTaskToBeScheduled, () -> {
              taskStartStatusLatches.get(nextTaskToBeScheduled).countDown();
              taskWaitLatches.get(nextTaskToBeScheduled).await();
              return null;
            });
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }));

        // wait for sometime to see new task didn't start as the throttler
        // can only run 5 tasks at a time which is already maxed out
        Assert.assertFalse(taskStartStatusLatches.get(nextTaskToBeScheduled).await(5, TimeUnit.SECONDS));

        // Finish one of the running tasks
        LOGGER.info("latch({}) countDown", currentTaskToBeExecuted);
        taskWaitLatches.get(currentTaskToBeExecuted).countDown();

        // test whether it finished running and everything else (apart from completedFutureidx) is not completed
        completedFutureidx.add(currentTaskToBeExecuted);
        TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
          Assert.assertTrue(taskFutures.get(currentTaskToBeExecuted).isDone());
          taskFutures.stream()
              .filter(future -> !completedFutureidx.contains(taskFutures.indexOf(future)))
              .forEach(future -> {
                Assert.assertFalse(future.isDone());
              });
        });

        // the new task should be scheduled as it's the only task next in line: wait for it to be scheduled
        taskStartStatusLatches.get(nextTaskToBeScheduled).await();
      }

      // 3. Once the initial 5 tasks are finished, throttler will be running the new 5 tasks,
      // Finish the new tasks as well.
      Assert.assertEquals(completedFutureidx.size(), 5);
      for (int i = 5; i < 10; i++) {
        int currentTaskToBeExecuted = i; // existing task to be finished

        // Finish one of the running tasks
        LOGGER.info("latch({}) countDown", currentTaskToBeExecuted);
        taskWaitLatches.get(currentTaskToBeExecuted).countDown();

        // test whether it finished running and everything else (apart from completedFutureidx) is not completed
        completedFutureidx.add(currentTaskToBeExecuted);
        TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
          Assert.assertTrue(taskFutures.get(currentTaskToBeExecuted).isDone());
          taskFutures.stream()
              .filter(future -> !completedFutureidx.contains(taskFutures.indexOf(future)))
              .forEach(future -> {
                Assert.assertFalse(future.isDone());
              });
        });
      }

      // 4. Verify all tasks are finished
      Assert.assertEquals(completedFutureidx.size(), 10);
    } finally {
      TestUtils.shutdownExecutor(executorService);
    }
  }
}
