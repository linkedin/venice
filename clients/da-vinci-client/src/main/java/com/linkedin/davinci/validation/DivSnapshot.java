package com.linkedin.davinci.validation;

import static com.linkedin.davinci.validation.PartitionTracker.TopicType;

import java.util.Optional;


/**
 * Wrapper class for {@link PartitionTracker} for the ConsumptionTask Thread to enqueue to the Drainer.
 * Contains the VT DIV (Segments) + LCVO and RT DIV (Segments) + LCRO and an Optional of RT TopicType.
 */
public class DivSnapshot {
  private final PartitionTracker partitionTracker;
  /** LCVO is located in the PartitionTracker. */
  private final long latestConsumedRtOffset;
  /** Will be empty if no RT segments exist in PartitionTracker and only VT segments exist. */
  private Optional<TopicType> rtTopicType;

  public DivSnapshot(PartitionTracker partitionTracker, long latestConsumedRtOffset, Optional<TopicType> rtTopicType) {
    this.partitionTracker = partitionTracker;
    this.latestConsumedRtOffset = latestConsumedRtOffset;
    this.rtTopicType = rtTopicType;
  }

  public PartitionTracker getPartitionTracker() {
    return partitionTracker;
  }

  public long getLatestConsumedRtOffset() {
    return latestConsumedRtOffset;
  }

  public Optional<TopicType> getRtTopicType() {
    return rtTopicType;
  }
}
