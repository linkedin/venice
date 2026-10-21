package com.linkedin.venice.utils.concurrent;

import static com.linkedin.venice.utils.concurrent.BlockingQueueType.ARRAY_BLOCKING_QUEUE;
import static com.linkedin.venice.utils.concurrent.BlockingQueueType.LINKED_BLOCKING_QUEUE;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.stats.ThreadPoolStats;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ThreadPoolFactoryTest {
  @Test
  public void testGetLinkedBlockingQueue() {
    ThreadPoolExecutor executor = ThreadPoolFactory.createThreadPool(10, "ThreadPoolName1", 10, LINKED_BLOCKING_QUEUE);
    Assert.assertTrue(executor.getQueue() instanceof LinkedBlockingQueue);
    executor.shutdownNow();
  }

  @Test
  public void testGetArrayBlockingQueue() {
    ThreadPoolExecutor executor = ThreadPoolFactory.createThreadPool(10, "ThreadPoolName1", 10, ARRAY_BLOCKING_QUEUE);
    Assert.assertTrue(executor.getQueue() instanceof ArrayBlockingQueue);
    executor.shutdownNow();
  }

  @Test
  public void testInstrumentedCallerRunsOnSaturationAndDiscardsAfterShutdown() throws InterruptedException {
    ThreadPoolExecutor executor = ThreadPoolFactory.createThreadPool(1, "test", 1, LINKED_BLOCKING_QUEUE);
    ThreadPoolStats stats = mock(ThreadPoolStats.class);
    CountDownLatch workerStarted = new CountDownLatch(1);
    CountDownLatch releaseWorker = new CountDownLatch(1);
    try {
      ThreadPoolFactory.instrumentCallerRunsPolicy(executor, stats);
      ThreadPoolFactory.instrumentCallerRunsPolicy(executor, stats);
      executor.execute(() -> {
        workerStarted.countDown();
        try {
          releaseWorker.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError(e);
        }
      });
      Assert.assertTrue(workerStarted.await(10, TimeUnit.SECONDS));
      executor.execute(() -> {});
      Thread caller = Thread.currentThread();
      executor.execute(() -> Assert.assertSame(Thread.currentThread(), caller));

      verify(stats).recordRejectedTask();
      verify(stats).recordCallerRun();
      ArgumentCaptor<Double> elapsed = ArgumentCaptor.forClass(Double.class);
      verify(stats).recordCallerRunTime(elapsed.capture());
      Assert.assertTrue(elapsed.getValue() >= 0);

      executor.shutdown();
      AtomicBoolean executedAfterShutdown = new AtomicBoolean();
      executor.execute(() -> executedAfterShutdown.set(true));
      Assert.assertFalse(executedAfterShutdown.get());
      verify(stats, times(2)).recordRejectedTask();
      verify(stats).recordCallerRun();
    } finally {
      releaseWorker.countDown();
      executor.shutdown();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testCallerRunsFailureStillRecordsDuration() {
    ThreadPoolExecutor executor = ThreadPoolFactory.createThreadPool(1, "test", 1, LINKED_BLOCKING_QUEUE);
    ThreadPoolStats stats = mock(ThreadPoolStats.class);
    try {
      ThreadPoolFactory.instrumentCallerRunsPolicy(executor, stats);
      RuntimeException failure = new RuntimeException("test failure");
      RuntimeException actual = Assert
          .expectThrows(RuntimeException.class, () -> executor.getRejectedExecutionHandler().rejectedExecution(() -> {
            throw failure;
          }, executor));
      Assert.assertSame(actual, failure);
      verify(stats).recordRejectedTask();
      verify(stats).recordCallerRun();
      verify(stats).recordCallerRunTime(anyDouble());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testCustomRejectionPolicyIsPreserved() {
    ThreadPoolExecutor executor = ThreadPoolFactory.createThreadPool(1, "test", 1, LINKED_BLOCKING_QUEUE);
    ThreadPoolStats stats = mock(ThreadPoolStats.class);
    RejectedExecutionHandler handler = new ThreadPoolExecutor.AbortPolicy();
    try {
      executor.setRejectedExecutionHandler(handler);
      ThreadPoolFactory.instrumentCallerRunsPolicy(executor, stats);
      Assert.assertSame(executor.getRejectedExecutionHandler(), handler);
      Assert.expectThrows(
          RejectedExecutionException.class,
          () -> executor.getRejectedExecutionHandler().rejectedExecution(() -> {}, executor));
      verify(stats, never()).recordRejectedTask();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testCustomCallerRunsPolicySubclassIsPreserved() {
    ThreadPoolExecutor executor = ThreadPoolFactory.createThreadPool(1, "test", 1, LINKED_BLOCKING_QUEUE);
    ThreadPoolStats stats = mock(ThreadPoolStats.class);
    AtomicBoolean customHandlerCalled = new AtomicBoolean();
    RejectedExecutionHandler handler = new ThreadPoolExecutor.CallerRunsPolicy() {
      @Override
      public void rejectedExecution(Runnable task, ThreadPoolExecutor pool) {
        customHandlerCalled.set(true);
        super.rejectedExecution(task, pool);
      }
    };
    try {
      executor.setRejectedExecutionHandler(handler);
      ThreadPoolFactory.instrumentCallerRunsPolicy(executor, stats);
      Assert.assertSame(executor.getRejectedExecutionHandler(), handler);
      executor.getRejectedExecutionHandler().rejectedExecution(() -> {}, executor);
      Assert.assertTrue(customHandlerCalled.get());
      verify(stats, never()).recordRejectedTask();
    } finally {
      executor.shutdownNow();
    }
  }
}
