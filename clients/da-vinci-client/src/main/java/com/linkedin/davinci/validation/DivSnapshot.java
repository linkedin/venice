package com.linkedin.davinci.validation;

/**
 * Wrapper class for {@link PartitionTracker} with latestConsumedRtOffset (LCRO) for the ConsumptionTask to enqueue
 * to the Drainer. Contains the VT DIV (Segments) + LCVO and RT DIV (Segments) + LCRO.
 */
public class DivSnapshot {
  public PartitionTracker partitionTracker;
  public long latestConsumedRtOffset; // LCVO is in PartitionTracker

  public DivSnapshot(PartitionTracker partitionTracker, long latestConsumedRtOffset) {
    this.partitionTracker = partitionTracker;
    this.latestConsumedRtOffset = latestConsumedRtOffset;
  }
}
