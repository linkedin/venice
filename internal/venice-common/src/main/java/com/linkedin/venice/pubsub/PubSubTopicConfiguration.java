package com.linkedin.venice.pubsub;

import java.util.Optional;


/**
 * Represents a {@link com.linkedin.venice.pubsub.api.PubSubTopic} configuration.
 */
public class PubSubTopicConfiguration {
  Optional<Long> retentionInMs;
  boolean isLogCompacted;
  Long minLogCompactionLagMs;
  Optional<Integer> minInSyncReplicas;

  public PubSubTopicConfiguration(
      Optional<Long> retentionInMs,
      boolean isLogCompacted,
      Optional<Integer> minInSyncReplicas,
      Long minLogCompactionLagMs) {
    this.retentionInMs = retentionInMs;
    this.isLogCompacted = isLogCompacted;
    this.minInSyncReplicas = minInSyncReplicas;
    this.minLogCompactionLagMs = minLogCompactionLagMs;
  }

  /**
   * @return whether the topic is log compacted
   */
  public boolean isLogCompacted() {
    return isLogCompacted;
  }

  /**
   * @return min number of replicas that must be in sync before a message is considered committed
   */
  public Optional<Integer> minInSyncReplicas() {
    return minInSyncReplicas;
  }

  /**
   * @return retention time for data in this topic
   */
  public Optional<Long> retentionInMs() {
    return retentionInMs;
  }

  /**
   * @return min log compaction lag in ms
   */
  public Long minLogCompactionLagMs() {
    return minLogCompactionLagMs;
  }

  /**
   * @param isLogCompacted whether the topic is log compacted
   */
  public void setLogCompacted(boolean isLogCompacted) {
    this.isLogCompacted = isLogCompacted;
  }

  /**
   * @param retentionInMs retention time for data in this topic
   */
  public void setRetentionInMs(Optional<Long> retentionInMs) {
    this.retentionInMs = retentionInMs;
  }

  /**
   * @param minInSyncReplicas min number of replicas that must be in sync before a message is considered committed
   */
  public void setMinInSyncReplicas(Optional<Integer> minInSyncReplicas) {
    this.minInSyncReplicas = minInSyncReplicas;
  }

  /**
   * @param minLogCompactionLagMs min log compaction lag in ms
   */
  public void setMinLogCompactionLagMs(Long minLogCompactionLagMs) {
    this.minLogCompactionLagMs = minLogCompactionLagMs;
  }

  @Override
  public String toString() {
    return String.format(
        "TopicConfiguration(retentionInMs = %s, isLogCompacted = %s, minInSyncReplicas = %s, minLogCompactionLagMs = %s)",
        retentionInMs,
        isLogCompacted,
        minInSyncReplicas,
        minLogCompactionLagMs);
  }
}
