package com.linkedin.davinci.kafka.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * Allocated only for opted-in partitions. Byte counters are deliberately not used here: Global DIV byte counters
 * reset on send, before durability. Source tokens are captured with the corresponding immutable DIV snapshot.
 */
class CheckpointProgress {
  final Object producerLock = new Object();
  final Object executionLock = new Object();
  private volatile boolean valid = true;

  boolean isValid() {
    return valid;
  }

  void invalidate() {
    synchronized (executionLock) {
      valid = false;
    }
  }

  private final Map<String, PeriodicCheckpoint> sources = new HashMap<>();
  private CompletableFuture<Void> checkpoints = CompletableFuture.completedFuture(null);

  synchronized PeriodicCheckpoint source(String key) {
    return sources.get(key);
  }

  synchronized PeriodicCheckpoint source(String key, PeriodicCheckpoint initial) {
    return sources.computeIfAbsent(key, ignored -> initial);
  }

  synchronized boolean isDue(long nowNs) {
    return valid && sources.values().stream().anyMatch(checkpoint -> checkpoint.isDue(nowNs));
  }

  synchronized Map<PeriodicCheckpoint, Long> capture(String source) {
    Map<PeriodicCheckpoint, Long> result = new HashMap<>();
    sources.forEach((key, checkpoint) -> {
      if (source == null || source.equals(key)) {
        result.put(checkpoint, checkpoint.generation());
      }
    });
    return result;
  }

  static void checkpointSucceeded(Map<PeriodicCheckpoint, Long> coverage) {
    long now = System.nanoTime();
    coverage.forEach((checkpoint, generation) -> checkpoint.checkpointSucceeded(generation, now));
  }

  synchronized void trackCheckpoint(CompletableFuture<Void> future) {
    checkpoints = combine(checkpoints, future);
  }

  synchronized CompletableFuture<Void> checkpoints() {
    return checkpoints;
  }

  private static CompletableFuture<Void> combine(CompletableFuture<Void> previous, CompletableFuture<Void> next) {
    // Retain a failure until the PCS is replaced; do not hide it behind a later success.
    return previous.isDone() && !previous.isCompletedExceptionally() ? next : CompletableFuture.allOf(previous, next);
  }
}
