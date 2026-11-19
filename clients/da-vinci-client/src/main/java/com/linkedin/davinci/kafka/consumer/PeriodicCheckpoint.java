package com.linkedin.davinci.kafka.consumer;

import java.util.concurrent.CompletableFuture;


/**
 * One subscription's user-data progress and monotonic freshness. Consumer, producer callback and drainer threads
 * share this state. Only a successful storage AND metadata checkpoint may acknowledge a captured generation.
 */
class PeriodicCheckpoint {
  interface CheckpointAction {
    /** A null result defers the attempt without acknowledging progress. */
    CompletableFuture<Void> checkpoint() throws InterruptedException;
  }

  private final long intervalNs;
  private final long spreadNs;
  private long deadlineNs;
  private long generation;
  private long checkpointedGeneration;
  private CompletableFuture<Void> inFlight;

  PeriodicCheckpoint(long intervalNs, long initialSpreadNs, long nowNs) {
    this.intervalNs = intervalNs;
    this.spreadNs = initialSpreadNs;
    this.deadlineNs = nowNs + intervalNs + spreadNs;
  }

  synchronized void recordUserData() {
    generation++;
  }

  synchronized long generation() {
    return generation;
  }

  synchronized boolean isDue(long nowNs) {
    return generation > checkpointedGeneration && nowNs - deadlineNs >= 0;
  }

  synchronized void checkpointSucceeded(long coveredGeneration, long nowNs) {
    if (coveredGeneration < checkpointedGeneration) {
      return;
    }
    checkpointedGeneration = Math.min(coveredGeneration, generation);
    // Keep the subscription's spread after EVERY success, including after missed deadlines or byte checkpoints.
    // Enqueue, skipped commands and producer acknowledgements never move this deadline.
    deadlineNs = nowNs + intervalNs + spreadNs;
  }

  void maybeCheckpoint(long nowNs, CheckpointAction action) throws InterruptedException {
    synchronized (this) {
      if (inFlight != null) {
        if (!inFlight.isDone()) {
          return;
        }
        CompletableFuture<Void> completed = inFlight;
        inFlight = null;
        completed.join();
      }
      if (!isDue(nowNs)) {
        return;
      }
    }
    // Never hold the progress monitor while putting into a bounded drainer queue.
    CompletableFuture<Void> future = action.checkpoint();
    synchronized (this) {
      inFlight = future;
    }
  }
}
