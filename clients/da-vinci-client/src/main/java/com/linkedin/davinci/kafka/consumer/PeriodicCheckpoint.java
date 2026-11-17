package com.linkedin.davinci.kafka.consumer;

import java.util.concurrent.CompletableFuture;


/**
 * Consumer-thread-owned deadline with at most one outstanding checkpoint. Uses monotonic time, not record timestamps.
 */
class PeriodicCheckpoint {
  interface CheckpointAction {
    /** A null result defers the attempt until a later poll without advancing the deadline. */
    CompletableFuture<Void> checkpoint() throws InterruptedException;
  }

  private final long intervalNs;
  private long deadlineNs;
  private CompletableFuture<Void> inFlight;

  PeriodicCheckpoint(long intervalNs, long initialSpreadNs, long nowNs) {
    this.intervalNs = intervalNs;
    this.deadlineNs = nowNs + intervalNs + initialSpreadNs;
  }

  void maybeCheckpoint(long nowNs, CheckpointAction action) throws InterruptedException {
    if (inFlight != null) {
      if (!inFlight.isDone()) {
        return;
      }
      CompletableFuture<Void> completed = inFlight;
      inFlight = null;
      completed.join();
    }
    if (nowNs - deadlineNs < 0) {
      return;
    }
    CompletableFuture<Void> future = action.checkpoint();
    if (future != null) {
      inFlight = future;
      deadlineNs += intervalNs;
      // Skip missed rounds rather than replaying them after backpressure or a long poll.
      if (nowNs - deadlineNs >= 0) {
        deadlineNs = nowNs + intervalNs;
      }
    }
  }
}
