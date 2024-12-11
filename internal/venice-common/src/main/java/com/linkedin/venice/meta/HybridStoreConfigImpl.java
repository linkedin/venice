package com.linkedin.venice.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.systemstore.schemas.StoreHybridConfig;
import com.linkedin.venice.utils.AvroCompatibilityUtils;
import com.linkedin.venice.utils.Time;
import java.util.Objects;


/**
 * A container of Hybrid Store related configurations.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HybridStoreConfigImpl implements HybridStoreConfig {
  public static final long DEFAULT_REWIND_TIME_IN_SECONDS = Time.SECONDS_PER_DAY;
  public static final long DEFAULT_HYBRID_TIME_LAG_THRESHOLD = -1L;
  public static final long DEFAULT_HYBRID_OFFSET_LAG_THRESHOLD = 1000L;
  public static final String DEFAULT_REAL_TIME_TOPIC_NAME = "";

  private final StoreHybridConfig hybridConfig;

  public HybridStoreConfigImpl(
      long rewindTimeInSeconds,
      long offsetLagThresholdToGoOnline,
      long producerTimestampLagThresholdToGoOnlineInSeconds,
      DataReplicationPolicy dataReplicationPolicy,
      BufferReplayPolicy bufferReplayPolicy) {
    this(
        rewindTimeInSeconds,
        offsetLagThresholdToGoOnline,
        producerTimestampLagThresholdToGoOnlineInSeconds,
        dataReplicationPolicy,
        bufferReplayPolicy,
        DEFAULT_REAL_TIME_TOPIC_NAME);
  }

  @JsonCreator
  public HybridStoreConfigImpl(
      @JsonProperty("rewindTimeInSeconds") long rewindTimeInSeconds,
      @JsonProperty("offsetLagThresholdToGoOnline") long offsetLagThresholdToGoOnline,
      @JsonProperty("producerTimestampLagThresholdToGoOnlineInSeconds") long producerTimestampLagThresholdToGoOnlineInSeconds,
      @JsonProperty("dataReplicationPolicy") DataReplicationPolicy dataReplicationPolicy,
      @JsonProperty("bufferReplayPolicy") BufferReplayPolicy bufferReplayPolicy,
      @JsonProperty("realTimeTopicName") String realTimeTopicName) {
    this.hybridConfig = new StoreHybridConfig();
    this.hybridConfig.rewindTimeInSeconds = rewindTimeInSeconds;
    this.hybridConfig.offsetLagThresholdToGoOnline = offsetLagThresholdToGoOnline;
    this.hybridConfig.producerTimestampLagThresholdToGoOnlineInSeconds =
        producerTimestampLagThresholdToGoOnlineInSeconds;
    this.hybridConfig.dataReplicationPolicy = dataReplicationPolicy == null
        ? DataReplicationPolicy.NON_AGGREGATE.getValue() // for deserializing old hybrid config that didn't have data
                                                         // replication policy
        : dataReplicationPolicy.getValue();
    this.hybridConfig.bufferReplayPolicy =
        bufferReplayPolicy == null ? BufferReplayPolicy.REWIND_FROM_EOP.getValue() : bufferReplayPolicy.getValue();
    this.hybridConfig.realTimeTopicName = realTimeTopicName == null ? DEFAULT_REAL_TIME_TOPIC_NAME : realTimeTopicName;
  }

  HybridStoreConfigImpl(StoreHybridConfig config) {
    this.hybridConfig = config;
  }

  @Override
  public long getRewindTimeInSeconds() {
    return this.hybridConfig.rewindTimeInSeconds;
  }

  @Override
  public long getOffsetLagThresholdToGoOnline() {
    return this.hybridConfig.offsetLagThresholdToGoOnline;
  }

  @Override
  public void setRewindTimeInSeconds(long rewindTimeInSeconds) {
    this.hybridConfig.rewindTimeInSeconds = rewindTimeInSeconds;
  }

  @Override
  public void setOffsetLagThresholdToGoOnline(long offsetLagThresholdToGoOnline) {
    this.hybridConfig.offsetLagThresholdToGoOnline = offsetLagThresholdToGoOnline;
  }

  @Override
  public long getProducerTimestampLagThresholdToGoOnlineInSeconds() {
    return this.hybridConfig.producerTimestampLagThresholdToGoOnlineInSeconds;
  }

  @Override
  public DataReplicationPolicy getDataReplicationPolicy() {
    return DataReplicationPolicy.valueOf(this.hybridConfig.dataReplicationPolicy);
  }

  @Override
  public void setDataReplicationPolicy(DataReplicationPolicy dataReplicationPolicy) {
    this.hybridConfig.dataReplicationPolicy = dataReplicationPolicy.getValue();
  }

  @Override
  public BufferReplayPolicy getBufferReplayPolicy() {
    return BufferReplayPolicy.valueOf(this.hybridConfig.bufferReplayPolicy);
  }

  @Override
  public String getRealTimeTopicName() {
    return this.hybridConfig.realTimeTopicName.toString();
  }

  @Override
  public void setRealTimeTopicName(String realTimeTopicName) {
    this.hybridConfig.realTimeTopicName = realTimeTopicName;
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
    HybridStoreConfigImpl that = (HybridStoreConfigImpl) o;
    return AvroCompatibilityUtils.compare(hybridConfig, that.hybridConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hybridConfig);
  }

  @JsonIgnore
  public HybridStoreConfig clone() {
    return new HybridStoreConfigImpl(
        getRewindTimeInSeconds(),
        getOffsetLagThresholdToGoOnline(),
        getProducerTimestampLagThresholdToGoOnlineInSeconds(),
        getDataReplicationPolicy(),
        getBufferReplayPolicy(),
        getRealTimeTopicName());
  }
}
