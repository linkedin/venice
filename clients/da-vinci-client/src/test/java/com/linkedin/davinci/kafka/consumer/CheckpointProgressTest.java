package com.linkedin.davinci.kafka.consumer;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class CheckpointProgressTest {
  @Test
  public void testSnapshotCoverageIsSourceAwareAndGenerationBounded() {
    CheckpointProgress progress = new CheckpointProgress();
    PeriodicCheckpoint first = progress.source("first", new PeriodicCheckpoint(100, 0, 0));
    PeriodicCheckpoint second = progress.source("second", new PeriodicCheckpoint(100, 0, 0));
    first.recordUserData();
    second.recordUserData();
    Map<PeriodicCheckpoint, Long> coverage = progress.capture("first");
    first.recordUserData();
    coverage.forEach((checkpoint, generation) -> checkpoint.checkpointSucceeded(generation, 100));
    assertTrue(second.isDue(100), "Another source's durable DIV must not postpone this source");
    assertFalse(first.isDue(199));
    assertTrue(first.isDue(200), "New arrivals must remain pending");
    first.checkpointSucceeded(first.generation(), 200);
    assertFalse(first.isDue(1000));
  }

  @Test
  public void testCheckpointFenceRetainsFailure() {
    CheckpointProgress progress = new CheckpointProgress();
    CompletableFuture<Void> first = new CompletableFuture<>();
    CompletableFuture<Void> second = new CompletableFuture<>();
    progress.trackCheckpoint(first);
    progress.trackCheckpoint(second);
    second.complete(null);
    assertFalse(progress.checkpoints().isDone());
    first.completeExceptionally(new IllegalStateException("failed persistence"));
    assertTrue(progress.checkpoints().isCompletedExceptionally());
    progress.trackCheckpoint(CompletableFuture.completedFuture(null));
    assertTrue(progress.checkpoints().isCompletedExceptionally());
  }

  @Test
  public void testCheckpointFenceIncludesExecutingWork() {
    CheckpointProgress progress = new CheckpointProgress();
    CompletableFuture<Void> executing = new CompletableFuture<>();
    progress.trackCheckpoint(executing);
    progress.trackCheckpoint(CompletableFuture.completedFuture(null));
    assertFalse(progress.checkpoints().isDone());
    executing.complete(null);
    assertTrue(progress.checkpoints().isDone());
  }

  @Test
  public void testInvalidationWaitsForExecutingCommand() throws Exception {
    CheckpointProgress progress = new CheckpointProgress();
    CountDownLatch executing = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    CompletableFuture<Void> command = CompletableFuture.runAsync(() -> {
      synchronized (progress.executionLock) {
        executing.countDown();
        try {
          assertTrue(finish.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError(e);
        }
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
      assertFalse(invalidated.isDone());
      assertTrue(progress.isValid());
      finish.countDown();
      command.get(5, TimeUnit.SECONDS);
      invalidated.get(5, TimeUnit.SECONDS);
      assertFalse(progress.isValid());
    } finally {
      finish.countDown();
      command.get(5, TimeUnit.SECONDS);
    }
  }

}
