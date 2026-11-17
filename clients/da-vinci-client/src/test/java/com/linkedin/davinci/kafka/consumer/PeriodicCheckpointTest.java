package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.testng.annotations.Test;


public class PeriodicCheckpointTest {
  @Test
  public void testSpreadAndFixedCadence() throws Exception {
    PeriodicCheckpoint.CheckpointAction action = mock(PeriodicCheckpoint.CheckpointAction.class);
    when(action.checkpoint()).thenReturn(CompletableFuture.completedFuture(null));
    PeriodicCheckpoint first = new PeriodicCheckpoint(100, 0, 0);
    PeriodicCheckpoint second = new PeriodicCheckpoint(100, 20, 0);

    first.maybeCheckpoint(99, action);
    second.maybeCheckpoint(119, action);
    verify(action, never()).checkpoint();
    first.maybeCheckpoint(105, action);
    second.maybeCheckpoint(120, action);
    verify(action, times(2)).checkpoint();
    first.maybeCheckpoint(199, action);
    first.maybeCheckpoint(200, action);
    verify(action, times(3)).checkpoint();
  }

  @Test
  public void testOutstandingCheckpointIsBoundedAndMissedRoundsAreSkipped() throws Exception {
    PeriodicCheckpoint.CheckpointAction action = mock(PeriodicCheckpoint.CheckpointAction.class);
    CompletableFuture<Void> pending = new CompletableFuture<>();
    when(action.checkpoint()).thenReturn(pending).thenReturn(CompletableFuture.completedFuture(null));
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 0, 0);
    checkpoint.maybeCheckpoint(100, action);
    checkpoint.maybeCheckpoint(500, action);
    verify(action, times(1)).checkpoint();
    pending.complete(null);
    checkpoint.maybeCheckpoint(500, action);
    checkpoint.maybeCheckpoint(501, action);
    checkpoint.maybeCheckpoint(599, action);
    verify(action, times(2)).checkpoint();
    checkpoint.maybeCheckpoint(600, action);
    verify(action, times(3)).checkpoint();
  }

  @Test
  public void testDeferralAndFailurePropagation() throws Exception {
    PeriodicCheckpoint.CheckpointAction action = mock(PeriodicCheckpoint.CheckpointAction.class);
    CompletableFuture<Void> failed = new CompletableFuture<>();
    failed.completeExceptionally(new IllegalStateException("checkpoint failed"));
    when(action.checkpoint()).thenReturn(null).thenReturn(failed);
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 0, 0);
    checkpoint.maybeCheckpoint(100, action);
    checkpoint.maybeCheckpoint(101, action);
    assertThrows(CompletionException.class, () -> checkpoint.maybeCheckpoint(102, action));
    verify(action, times(2)).checkpoint();
  }

  @Test
  public void testMonotonicClockWraparound() throws Exception {
    PeriodicCheckpoint.CheckpointAction action = mock(PeriodicCheckpoint.CheckpointAction.class);
    when(action.checkpoint()).thenReturn(CompletableFuture.completedFuture(null));
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 0, Long.MAX_VALUE - 50);
    checkpoint.maybeCheckpoint(Long.MIN_VALUE + 48, action);
    verify(action, never()).checkpoint();
    checkpoint.maybeCheckpoint(Long.MIN_VALUE + 49, action);
    verify(action).checkpoint();
  }
}
