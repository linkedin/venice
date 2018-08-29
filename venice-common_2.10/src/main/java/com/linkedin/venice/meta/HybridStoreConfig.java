package com.linkedin.venice.meta;

import org.codehaus.jackson.annotate.JsonProperty;


/**
 * A container of Hybrid Store related configurations.
 */
//TODO, converge on fasterxml or codehouse
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
@org.codehaus.jackson.annotate.JsonIgnoreProperties(ignoreUnknown = true)
public class HybridStoreConfig {
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
