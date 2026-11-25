package com.linkedin.davinci.kafka.consumer;

/**
 * One subscription's monotonic deadline. The caller supplies pending bytes from the existing checkpoint counter.
 */
class PeriodicCheckpoint {
  private final long intervalNs;
  private final long spreadNs;
  private long deadlineNs;

  PeriodicCheckpoint(long intervalNs, long initialSpreadNs, long nowNs) {
    this.intervalNs = intervalNs;
    this.spreadNs = initialSpreadNs;
    this.deadlineNs = nowNs + intervalNs + spreadNs;
  }

  synchronized boolean isDue(long nowNs, long pendingBytes) {
    return pendingBytes > 0 && nowNs - deadlineNs >= 0;
  }

  synchronized void checkpointSucceeded(long nowNs) {
    // Keep the subscription's spread after EVERY success, including after missed deadlines or byte checkpoints.
    // Enqueue, skipped commands and producer acknowledgements never move this deadline.
    deadlineNs = nowNs + intervalNs + spreadNs;
  }

}
