package com.linkedin.venice.meta;

import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.utils.Time;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * A container of Hybrid Store related configurations.
 */
//TODO, converge on fasterxml or codehouse
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
@org.codehaus.jackson.annotate.JsonIgnoreProperties(ignoreUnknown = true)
public class HybridStoreConfig {
  private static final long BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN = 2 * Time.MS_PER_DAY;
  private long rewindTimeInSeconds;
  private long offsetLagThresholdToGoOnline;

  public HybridStoreConfig(
      @JsonProperty("rewindTimeInSeconds") @com.fasterxml.jackson.annotation.JsonProperty("rewindTimeInSeconds") long rewindTimeInSeconds,
      @JsonProperty("offsetLagThresholdToGoOnline") @com.fasterxml.jackson.annotation.JsonProperty("offsetLagThresholdToGoOnline") long offsetLagThresholdToGoOnline
  ) {
    this.rewindTimeInSeconds = rewindTimeInSeconds;
    this.offsetLagThresholdToGoOnline = offsetLagThresholdToGoOnline;
  }

  public long getRewindTimeInSeconds() {
    return rewindTimeInSeconds;
  }

  /**
   * The default retention time for the RT topic is defined in {@link TopicManager#DEFAULT_TOPIC_RETENTION_POLICY_MS},
   * but if the rewind time is larger than this, then the RT topic's retention time needs to be set even higher,
   * in order to guarantee that buffer replays do not lose data. In order to achieve this, the retention time is
   * set to the max of either:
   *
   * 1. {@link TopicManager#DEFAULT_TOPIC_RETENTION_POLICY_MS}; or
   * 2. {@link #rewindTimeInSeconds} + {@value #BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN};
   *
   * This is a convenience function, and thus must be ignored by the JSON machinery.
   *
   * @return the retention time for the RT topic, in milliseconds.
   */
  @JsonIgnore
  @com.fasterxml.jackson.annotation.JsonIgnore
  public long getRetentionTimeInMs() {
    long rewindTimeInMs = rewindTimeInSeconds * Time.MS_PER_SECOND;
    long minimumRetentionInMs = rewindTimeInMs + BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN;
    return Math.max(minimumRetentionInMs, TopicManager.DEFAULT_TOPIC_RETENTION_POLICY_MS);
  }


  public long getOffsetLagThresholdToGoOnline() {
    return offsetLagThresholdToGoOnline;
  }

  public void setRewindTimeInSeconds(long rewindTimeInSeconds) {
    this.rewindTimeInSeconds = rewindTimeInSeconds;
  }

  public void setOffsetLagThresholdToGoOnline(long offsetLagThresholdToGoOnline) {
    this.offsetLagThresholdToGoOnline = offsetLagThresholdToGoOnline;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    HybridStoreConfig that = (HybridStoreConfig) o;

    if (rewindTimeInSeconds != that.rewindTimeInSeconds) return false;
    return offsetLagThresholdToGoOnline == that.offsetLagThresholdToGoOnline;
  }

  @Override
  public int hashCode() {
    int result = (int) (rewindTimeInSeconds ^ (rewindTimeInSeconds >>> 32));
    result = 31 * result + (int) (offsetLagThresholdToGoOnline ^ (offsetLagThresholdToGoOnline >>> 32));
    return result;
  }

  @com.fasterxml.jackson.annotation.JsonIgnore
  @org.codehaus.jackson.annotate.JsonIgnore
  public HybridStoreConfig clone(){
    return new HybridStoreConfig(rewindTimeInSeconds, offsetLagThresholdToGoOnline);
  }
}
