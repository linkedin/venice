package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
  public void testOnlyDirtyAndDueProgressTriggers() throws Exception {
    PeriodicCheckpoint.CheckpointAction action = mock(PeriodicCheckpoint.CheckpointAction.class);
    when(action.checkpoint()).thenReturn(CompletableFuture.completedFuture(null));
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 20, 0);
    checkpoint.maybeCheckpoint(1000, action);
    verify(action, never()).checkpoint();
    checkpoint.recordUserData();
    checkpoint.maybeCheckpoint(119, action);
    verify(action, never()).checkpoint();
    checkpoint.maybeCheckpoint(120, action);
    verify(action).checkpoint();
    // Completing a command without a successful sync is not an acknowledgement.
    checkpoint.maybeCheckpoint(121, action);
    verify(action, times(2)).checkpoint();
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
  public void testOutstandingCheckpointDefersAndPreservesPhaseAcrossMissedRounds() throws Exception {
    PeriodicCheckpoint.CheckpointAction action = mock(PeriodicCheckpoint.CheckpointAction.class);
    CompletableFuture<Void> pending = new CompletableFuture<>();
    when(action.checkpoint()).thenReturn(pending);
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 20, 0);
    checkpoint.recordUserData();
    checkpoint.maybeCheckpoint(120, action);
    checkpoint.maybeCheckpoint(500, action);
    verify(action).checkpoint();
    checkpoint.checkpointSucceeded(checkpoint.generation(), 501);
    pending.complete(null);
    checkpoint.recordUserData();
    checkpoint.maybeCheckpoint(620, action);
    verify(action).checkpoint();
    checkpoint.maybeCheckpoint(621, action);
    verify(action, times(2)).checkpoint();
  }

  @Test
  public void testDeferralAndFailureNeverAcknowledgeProgress() throws Exception {
    PeriodicCheckpoint.CheckpointAction action = mock(PeriodicCheckpoint.CheckpointAction.class);
    CompletableFuture<Void> failed = new CompletableFuture<>();
    failed.completeExceptionally(new IllegalStateException("checkpoint failed"));
    when(action.checkpoint()).thenReturn(null).thenReturn(failed);
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 0, 0);
    checkpoint.recordUserData();
    checkpoint.maybeCheckpoint(100, action);
    checkpoint.maybeCheckpoint(101, action);
    assertThrows(CompletionException.class, () -> checkpoint.maybeCheckpoint(102, action));
    assertTrue(checkpoint.isDue(102));
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
