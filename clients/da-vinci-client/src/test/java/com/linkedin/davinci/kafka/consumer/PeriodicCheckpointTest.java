package com.linkedin.davinci.kafka.consumer;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;


public class PeriodicCheckpointTest {
  @Test
  public void testControlCheckpointRefreshesFreshnessWithoutDirtyingCleanSubscription() {
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 0, 0);
    checkpoint.checkpointSucceeded(0, 200);
    assertFalse(checkpoint.isDue(1000));
    checkpoint.recordUserData();
    assertFalse(checkpoint.isDue(299));
    assertTrue(checkpoint.isDue(300));
  }

  @Test
  public void testOnlyPendingDataAfterIntervalAndStableSpreadIsDue() {
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 20, 0);
    assertFalse(checkpoint.isDue(1000));
    checkpoint.recordUserData();
    assertFalse(checkpoint.isDue(119));
    assertTrue(checkpoint.isDue(120));
  }

  @Test
  public void testSuccessfulByteCheckpointRefreshesFreshnessWithoutClearingNewerData() {
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 20, 0);
    checkpoint.recordUserData();
    long covered = checkpoint.generation();
    checkpoint.recordUserData();
    checkpoint.checkpointSucceeded(covered, 125);
    assertFalse(checkpoint.isDue(244));
    assertTrue(checkpoint.isDue(245));
    checkpoint.checkpointSucceeded(checkpoint.generation(), 245);
    assertFalse(checkpoint.isDue(1000));
    // Stale completion cannot postpone a new generation.
    checkpoint.recordUserData();
    checkpoint.checkpointSucceeded(covered, 999);
    assertTrue(checkpoint.isDue(1000));
  }

  @Test
  public void testOutstandingSnapshotDefersTimeBranchWithoutAcknowledgingData() {
    CompletableFuture<Void> pending = new CompletableFuture<>();
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 20, 0);
    checkpoint.recordUserData();
    assertTrue(checkpoint.isDue(120));
    checkpoint.trackCheckpoint(pending);
    assertFalse(checkpoint.isDue(500));
    checkpoint.checkpointSucceeded(checkpoint.generation(), 501);
    pending.complete(null);
    checkpoint.recordUserData();
    assertFalse(checkpoint.isDue(620));
    assertTrue(checkpoint.isDue(621));
  }

  @Test
  public void testFailureAndNoOpNeverRefreshDeadline() {
    CompletableFuture<Void> failed = new CompletableFuture<>();
    failed.completeExceptionally(new IllegalStateException("checkpoint failed"));
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 0, 0);
    checkpoint.recordUserData();
    checkpoint.trackCheckpoint(CompletableFuture.completedFuture(null));
    assertTrue(checkpoint.isDue(100));
    checkpoint.trackCheckpoint(failed);
    assertTrue(checkpoint.isDue(102));
  }

  @Test
  public void testEmptyCoverageCannotPostponeUncoveredData() {
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 0, 0);
    checkpoint.recordUserData();
    checkpoint.checkpointSucceeded(0, 200);
    assertTrue(checkpoint.isDue(100));
  }

  @Test
  public void testMonotonicClockWraparound() {
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 0, Long.MAX_VALUE - 50);
    checkpoint.recordUserData();
    assertFalse(checkpoint.isDue(Long.MIN_VALUE + 48));
    assertTrue(checkpoint.isDue(Long.MIN_VALUE + 49));
    checkpoint.checkpointSucceeded(checkpoint.generation(), Long.MIN_VALUE + 50);
    checkpoint.recordUserData();
    assertFalse(checkpoint.isDue(Long.MIN_VALUE + 149));
    assertTrue(checkpoint.isDue(Long.MIN_VALUE + 150));
  }
}
