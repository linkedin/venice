package com.linkedin.venice.vpj.pubsub.input;

import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_PUBSUB_INPUT_MAX_RECORDS_PER_MAPPER;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import java.time.Duration;
import java.util.Objects;


public final class SplitRequest {
  public static final long DEFAULT_RECORDS_PER_SPLIT = DEFAULT_PUBSUB_INPUT_MAX_RECORDS_PER_MAPPER;
  public static final long DEFAULT_TIME_WINDOW_MS = Duration.ofHours(3).toMillis();
  public static final int DEFAULT_MAX_SPLITS = 5;

  private final PubSubTopicPartition pubSubTopicPartition;
  private final TopicManager topicManager;
  private final PartitionSplitStrategy partitionSplitStrategy;

  // Optionals (boxed so null means unset)
  private final Integer maxSplits; // for CAPPED_SPLIT_COUNT
  private final Long recordsPerSplit; // for FIXED_RECORD_COUNT
  private final Long timeWindowInMs; // for TIME_WINDOW

  private SplitRequest(Builder builder) {
    this.pubSubTopicPartition = builder.pubSubTopicPartition;
    this.topicManager = builder.topicManager;
    this.partitionSplitStrategy = builder.partitionSplitStrategy;
    this.maxSplits = builder.maxSplits;
    this.recordsPerSplit = builder.recordsPerSplit;
    this.timeWindowInMs = builder.timeWindowInMs;
  }

  public PubSubTopicPartition getPubSubTopicPartition() {
    return pubSubTopicPartition;
  }

  public TopicManager getTopicManager() {
    return topicManager;
  }

  public PartitionSplitStrategy getSplitType() {
    return partitionSplitStrategy;
  }

  public Integer getMaxSplits() {
    return maxSplits;
  }

  public long getRecordsPerSplit() {
    return recordsPerSplit;
  }

  public long getTimeWindowInMs() {
    return timeWindowInMs;
  }

  @Override
  public String toString() {
    return "SplitRequest{" + "pubSubTopicPartition=" + pubSubTopicPartition + ", partitionSplitStrategy="
        + partitionSplitStrategy + ", maxSplits=" + maxSplits + ", recordsPerSplit=" + recordsPerSplit
        + ", timeWindowInMs=" + timeWindowInMs + '}';
  }

  public static final class Builder {
    private PubSubTopicPartition pubSubTopicPartition;
    private TopicManager topicManager;
    private PartitionSplitStrategy partitionSplitStrategy = PartitionSplitStrategy.FIXED_RECORD_COUNT; // Default split
                                                                                                       // type

    private Integer maxSplits;
    private Long recordsPerSplit;
    private Long timeWindowInMs;

    public Builder() {
    }

    public Builder pubSubTopicPartition(PubSubTopicPartition value) {
      this.pubSubTopicPartition = value;
      return this;
    }

    public Builder topicManager(TopicManager value) {
      this.topicManager = value;
      return this;
    }

    public Builder splitType(PartitionSplitStrategy value) {
      this.partitionSplitStrategy = value;
      return this;
    }

    /** Used when partitionSplitStrategy is CAPPED_SPLIT_COUNT. */
    public Builder maxSplits(Integer value) {
      this.maxSplits = value;
      return this;
    }

    /** Used when partitionSplitStrategy is FIXED_RECORD_COUNT. */
    public Builder recordsPerSplit(long value) {
      this.recordsPerSplit = value;
      return this;
    }

    /** Used when partitionSplitStrategy is TIME_WINDOW. */
    public Builder timeWindowInMs(long value) {
      this.timeWindowInMs = value;
      return this;
    }

    private void validate() {
      Objects.requireNonNull(pubSubTopicPartition, "pubSubTopicPartition");
      Objects.requireNonNull(topicManager, "topicManager");
      Objects.requireNonNull(partitionSplitStrategy, "partitionSplitStrategy");

      switch (partitionSplitStrategy) {
        case CAPPED_SPLIT_COUNT:
          if (maxSplits == null || maxSplits <= 0) {
            maxSplits = DEFAULT_MAX_SPLITS;
          }
          break;

        case FIXED_RECORD_COUNT:
          if (recordsPerSplit == null || recordsPerSplit <= 0) {
            recordsPerSplit = DEFAULT_RECORDS_PER_SPLIT;
          }
          break;

        case SINGLE_SPLIT_PER_PARTITION:
          // No extra parameters required.
          break;

        default:
          throw new IllegalStateException("Unknown PartitionSplitStrategy: " + partitionSplitStrategy);
      }
    }

    public SplitRequest build() {
      validate();
      return new SplitRequest(this);
    }
  }
}
