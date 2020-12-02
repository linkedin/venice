package com.linkedin.venice.meta;

import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.systemstore.schemas.StoreHybridConfig;
import com.linkedin.venice.utils.Time;
import java.util.Objects;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * A container of Hybrid Store related configurations.
 */
//TODO, converge on fasterxml or codehouse
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
@org.codehaus.jackson.annotate.JsonIgnoreProperties(ignoreUnknown = true)
public class HybridStoreConfig implements DataModelBackedStructure<StoreHybridConfig> {
  private static final long BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN = 2 * Time.MS_PER_DAY;
  public static final long DEFAULT_HYBRID_TIME_LAG_THRESHOLD = -1L;

  private final StoreHybridConfig hybridConfig;

  public HybridStoreConfig(
      @JsonProperty("rewindTimeInSeconds") @com.fasterxml.jackson.annotation.JsonProperty("rewindTimeInSeconds") long rewindTimeInSeconds,
      @JsonProperty("offsetLagThresholdToGoOnline") @com.fasterxml.jackson.annotation.JsonProperty("offsetLagThresholdToGoOnline") long offsetLagThresholdToGoOnline,
      @JsonProperty("producerTimestampLagThresholdToGoOnlineInSeconds") @com.fasterxml.jackson.annotation.JsonProperty("producerTimestampLagThresholdToGoOnlineInSeconds") long producerTimestampLagThresholdToGoOnlineInSeconds
  ) {
    this.hybridConfig = new StoreHybridConfig();
    this.hybridConfig.rewindTimeInSeconds = rewindTimeInSeconds;
    this.hybridConfig.offsetLagThresholdToGoOnline = offsetLagThresholdToGoOnline;
    this.hybridConfig.producerTimestampLagThresholdToGoOnlineInSeconds = producerTimestampLagThresholdToGoOnlineInSeconds;
  }

  HybridStoreConfig(StoreHybridConfig config) {
    this.hybridConfig = config;
  }

  public long getRewindTimeInSeconds() {
    return this.hybridConfig.rewindTimeInSeconds;
  }

  /**
   * The default retention time for the RT topic is defined in {@link TopicManager#DEFAULT_TOPIC_RETENTION_POLICY_MS},
   * but if the rewind time is larger than this, then the RT topic's retention time needs to be set even higher,
   * in order to guarantee that buffer replays do not lose data. In order to achieve this, the retention time is
   * set to the max of either:
   *
   * 1. {@link TopicManager#DEFAULT_TOPIC_RETENTION_POLICY_MS}; or
   * 2. {@link StoreHybridConfig#rewindTimeInSeconds} + {@value #BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN};
   *
   * This is a convenience function, and thus must be ignored by the JSON machinery.
   *
   * @return the retention time for the RT topic, in milliseconds.
   */
  @JsonIgnore
  @com.fasterxml.jackson.annotation.JsonIgnore
  public long getRetentionTimeInMs() {
    long rewindTimeInMs = this.hybridConfig.rewindTimeInSeconds * Time.MS_PER_SECOND;
    long minimumRetentionInMs = rewindTimeInMs + BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN;
    return Math.max(minimumRetentionInMs, TopicManager.DEFAULT_TOPIC_RETENTION_POLICY_MS);
  }

  public long getOffsetLagThresholdToGoOnline() {
    return this.hybridConfig.offsetLagThresholdToGoOnline;
  }

  public void setRewindTimeInSeconds(long rewindTimeInSeconds) {
    this.hybridConfig.rewindTimeInSeconds = rewindTimeInSeconds;
  }

  public void setOffsetLagThresholdToGoOnline(long offsetLagThresholdToGoOnline) {
    this.hybridConfig.offsetLagThresholdToGoOnline = offsetLagThresholdToGoOnline;
  }

  public long getProducerTimestampLagThresholdToGoOnlineInSeconds() {
    return this.hybridConfig.producerTimestampLagThresholdToGoOnlineInSeconds;
  }

  @Override
  public StoreHybridConfig dataModel() {
    return this.hybridConfig;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HybridStoreConfig that = (HybridStoreConfig) o;
    return hybridConfig.equals(that.hybridConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hybridConfig);
  }

  @com.fasterxml.jackson.annotation.JsonIgnore
  @org.codehaus.jackson.annotate.JsonIgnore
  public HybridStoreConfig clone(){
    return new HybridStoreConfig(getRewindTimeInSeconds(), getOffsetLagThresholdToGoOnline(), getProducerTimestampLagThresholdToGoOnlineInSeconds());
  }
}
