package com.linkedin.davinci.kafka.consumer;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class CheckpointProgressTest {
  @Test
  public void testSourceDeadlinesAreIndependent() {
    CheckpointProgress progress = new CheckpointProgress();
    PeriodicCheckpoint first = progress.source("first", new PeriodicCheckpoint(100, 0, 0));
    PeriodicCheckpoint second = progress.source("second", new PeriodicCheckpoint(100, 0, 0));
    first.checkpointSucceeded(100);
    assertTrue(second.isDue(100, 1), "Another source's successful DIV must not postpone this source");
    assertFalse(first.isDue(199, 1));
    assertTrue(first.isDue(200, 1));
    assertFalse(first.isDue(1000, 0));
  }

  @Test
  public void testPartitionSuccessRefreshesInitializedDeadlines() {
    CheckpointProgress progress = new CheckpointProgress();
    PeriodicCheckpoint checkpoint = progress.source("source", new PeriodicCheckpoint(100, 0, 0));
    progress.checkpointSucceeded(100);
    assertFalse(checkpoint.isDue(199, 1));
    assertTrue(checkpoint.isDue(200, 1));
  }

  @Test
  public void testInvalidationDisablesDueChecks() {
    CheckpointProgress progress = new CheckpointProgress();
    PeriodicCheckpoint checkpoint = progress.source("source", new PeriodicCheckpoint(100, 0, 0));
    assertTrue(progress.isDue(100, 1));
    progress.invalidate();
    assertFalse(progress.isDue(100, 1));
    org.testng.Assert.assertSame(progress.source("source"), checkpoint);
  }

  @Test
  public void testInvalidationDoesNotWaitForExecutingStorage() throws Exception {
    CheckpointProgress progress = new CheckpointProgress();
    CountDownLatch executing = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    CompletableFuture<Void> command = CompletableFuture.runAsync(() -> {
      progress.executionLock.lock();
      try {
        executing.countDown();
        assertTrue(finish.await(5, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      } finally {
        progress.executionLock.unlock();
      }
    });
    try {
      assertTrue(executing.await(5, TimeUnit.SECONDS));
      CountDownLatch invalidating = new CountDownLatch(1);
      CompletableFuture<Void> invalidated = CompletableFuture.runAsync(() -> {
        invalidating.countDown();
        progress.invalidate();
      });
      assertTrue(invalidating.await(5, TimeUnit.SECONDS));
      invalidated.get(5, TimeUnit.SECONDS);
      assertFalse(progress.isValid());
      assertFalse(progress.executionLock.tryLock(10, TimeUnit.MILLISECONDS));
      finish.countDown();
      command.get(5, TimeUnit.SECONDS);
      assertFalse(progress.isValid());
    } finally {
      finish.countDown();
      command.get(5, TimeUnit.SECONDS);
    }
  }

}
