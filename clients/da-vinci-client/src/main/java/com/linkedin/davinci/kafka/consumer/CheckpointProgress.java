package com.linkedin.davinci.kafka.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Allocated only for opted-in partitions. Tracks source deadlines and guards actual storage checkpoint execution.
 * Pending work is determined by the existing byte counters, not by this state.
 */
class CheckpointProgress {
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

  synchronized boolean isDue(long nowNs, long pendingBytes) {
    return valid && sources.values().stream().anyMatch(checkpoint -> checkpoint.isDue(nowNs, pendingBytes));
  }

  synchronized void checkpointSucceeded(long nowNs) {
    sources.values().forEach(checkpoint -> checkpoint.checkpointSucceeded(nowNs));
  }

  static class Snapshot {
    final PartitionConsumptionState pcs;
    final CheckpointProgress progress;
    final PeriodicCheckpoint source;

    Snapshot(PartitionConsumptionState pcs, PeriodicCheckpoint source) {
      this.pcs = pcs;
      this.progress = pcs.initializeCheckpointProgress();
      this.source = source;
    }

    void checkpointSucceeded(long nowNs) {
      if (source == null) {
        progress.checkpointSucceeded(nowNs);
      } else {
        source.checkpointSucceeded(nowNs);
      }
    }
  }
}
