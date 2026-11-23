package com.linkedin.davinci.kafka.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Allocated only for opted-in partitions. Byte counters are deliberately not used here: Global DIV byte counters
 * reset on send, before durability. Source tokens are captured with the corresponding immutable DIV snapshot.
 */
class CheckpointProgress {
  // Synchronous snapshot/cursor ordering only. These locks do not reserve asynchronous producer or queue work.
  final Object producerLock = new Object();
  final Object validationLock = new Object();
  final ReentrantLock executionLock = new ReentrantLock();
  private volatile boolean valid = true;

  boolean isValid() {
    return valid;
  }

  synchronized void invalidate() {
    valid = false;
  }

  private final Map<String, PeriodicCheckpoint> sources = new HashMap<>();

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

  static class Snapshot {
    final PartitionConsumptionState pcs;
    final CheckpointProgress progress;
    final Map<PeriodicCheckpoint, Long> coverage;
    final boolean timeOnly;

    Snapshot(PartitionConsumptionState pcs, Map<PeriodicCheckpoint, Long> coverage, boolean timeOnly) {
      this.pcs = pcs;
      this.progress = pcs.initializeCheckpointProgress();
      this.coverage = coverage;
      this.timeOnly = timeOnly;
    }

    boolean isDue(long nowNs) {
      return !timeOnly || coverage.entrySet()
          .stream()
          .anyMatch(entry -> entry.getKey().isCoveredGenerationDue(entry.getValue(), nowNs));
    }
  }
}
