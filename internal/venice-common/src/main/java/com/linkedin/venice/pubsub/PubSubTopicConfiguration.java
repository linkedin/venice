package com.linkedin.venice.pubsub;

import java.util.Optional;


/**
 * Represents a {@link com.linkedin.venice.pubsub.api.PubSubTopic} configuration.
 */
public class PubSubTopicConfiguration implements Cloneable {
  Optional<Long> retentionInMs;
  boolean isLogCompacted;
  Long minLogCompactionLagMs;
  Optional<Long> maxLogCompactionLagMs;
  Optional<Integer> minInSyncReplicas;

  public PubSubTopicConfiguration(
      Optional<Long> retentionInMs,
      boolean isLogCompacted,
      Optional<Integer> minInSyncReplicas,
      Long minLogCompactionLagMs,
      Optional<Long> maxLogCompactionLagMs) {
    this.retentionInMs = retentionInMs;
    this.isLogCompacted = isLogCompacted;
    this.minInSyncReplicas = minInSyncReplicas;
    this.minLogCompactionLagMs = minLogCompactionLagMs;
    this.maxLogCompactionLagMs = maxLogCompactionLagMs;
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

  public Optional<Long> getMaxLogCompactionLagMs() {
    return maxLogCompactionLagMs;
  }

  /**
   * The maximum time a message will remain ineligible for compaction in the log. Only applicable for logs that are being compacted.
   */
  public void setMaxLogCompactionLagMs(Optional<Long> maxLogCompactionLagMs) {
    this.maxLogCompactionLagMs = maxLogCompactionLagMs;
  }

  @Override
  public String toString() {
    return String.format(
        "TopicConfiguration(retentionInMs = %s, isLogCompacted = %s, minInSyncReplicas = %s, minLogCompactionLagMs = %s, maxLogCompactionLagMs = %s)",
        retentionInMs.isPresent() ? retentionInMs.get() : "not set",
        isLogCompacted,
        minInSyncReplicas.isPresent() ? minInSyncReplicas.get() : "not set",
        minLogCompactionLagMs,
        maxLogCompactionLagMs.isPresent() ? maxLogCompactionLagMs.get() : " not set");
  }

  @Override
  public PubSubTopicConfiguration clone() throws CloneNotSupportedException {
    return (PubSubTopicConfiguration) super.clone();
  }
}
