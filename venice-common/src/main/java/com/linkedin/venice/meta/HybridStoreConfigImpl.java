package com.linkedin.venice.meta;

import com.linkedin.venice.systemstore.schemas.StoreHybridConfig;
import com.linkedin.venice.utils.AvroCompatibilityUtils;
import java.util.Objects;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * A container of Hybrid Store related configurations.
 */
//TODO, converge on fasterxml or codehouse
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
@org.codehaus.jackson.annotate.JsonIgnoreProperties(ignoreUnknown = true)
public class HybridStoreConfigImpl implements HybridStoreConfig {
  public static final long DEFAULT_REWIND_TIME_IN_SECONDS = -1L;
  public static final long DEFAULT_HYBRID_TIME_LAG_THRESHOLD = -1L;
  public static final long DEFAULT_HYBRID_OFFSET_LAG_THRESHOLD = -1L;
  public static final long DEFAULT_BUFFER_REPLAY_REFERENCE_TIMESTAMP_MS = -1L;

  private final StoreHybridConfig hybridConfig;

  public HybridStoreConfigImpl(
      @JsonProperty("rewindTimeInSeconds") @com.fasterxml.jackson.annotation.JsonProperty("rewindTimeInSeconds") long rewindTimeInSeconds,
      @JsonProperty("offsetLagThresholdToGoOnline") @com.fasterxml.jackson.annotation.JsonProperty("offsetLagThresholdToGoOnline") long offsetLagThresholdToGoOnline,
      @JsonProperty("producerTimestampLagThresholdToGoOnlineInSeconds") @com.fasterxml.jackson.annotation.JsonProperty("producerTimestampLagThresholdToGoOnlineInSeconds") long producerTimestampLagThresholdToGoOnlineInSeconds,
      @JsonProperty("dataReplicationPolicy")@com.fasterxml.jackson.annotation.JsonProperty("dataReplicationPolicy") DataReplicationPolicy dataReplicationPolicy,
      @JsonProperty("bufferReplayPolicy")@com.fasterxml.jackson.annotation.JsonProperty("bufferReplayPolicy") BufferReplayPolicy bufferReplayPolicy
  ) {
    this.hybridConfig = new StoreHybridConfig();
    this.hybridConfig.rewindTimeInSeconds = rewindTimeInSeconds;
    this.hybridConfig.offsetLagThresholdToGoOnline = offsetLagThresholdToGoOnline;
    this.hybridConfig.producerTimestampLagThresholdToGoOnlineInSeconds = producerTimestampLagThresholdToGoOnlineInSeconds;
    this.hybridConfig.dataReplicationPolicy = dataReplicationPolicy == null
        ? DataReplicationPolicy.NON_AGGREGATE.getValue() // for deserializing old hybrid config that didn't have data replication policy
        : dataReplicationPolicy.getValue();
    this.hybridConfig.bufferReplayPolicy = bufferReplayPolicy == null ? BufferReplayPolicy.REWIND_FROM_EOP.getValue() :
        bufferReplayPolicy.getValue();
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
  public BufferReplayPolicy getBufferReplayPolicy() {
    return BufferReplayPolicy.valueOf(this.hybridConfig.bufferReplayPolicy);
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

  @com.fasterxml.jackson.annotation.JsonIgnore
  @org.codehaus.jackson.annotate.JsonIgnore
  public HybridStoreConfig clone(){
    return new HybridStoreConfigImpl(
        getRewindTimeInSeconds(),
        getOffsetLagThresholdToGoOnline(),
        getProducerTimestampLagThresholdToGoOnlineInSeconds(),
        getDataReplicationPolicy(),
        getBufferReplayPolicy());
  }
}
