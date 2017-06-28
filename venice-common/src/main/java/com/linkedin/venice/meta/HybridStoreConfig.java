package com.linkedin.venice.meta;

/**
 * A container of Hybrid Store related configurations.
 */
public class HybridStoreConfig {
  private final long rewindTimeInSeconds;
  private final long offsetLagThresholdToGoOnline;

  public HybridStoreConfig(long rewindTimeInSeconds, long offsetLagThresholdToGoOnline) {
    this.rewindTimeInSeconds = rewindTimeInSeconds;
    this.offsetLagThresholdToGoOnline = offsetLagThresholdToGoOnline;
  }

  public long getRewindTimeInSeconds() {
    return rewindTimeInSeconds;
  }

  public long getOffsetLagThresholdToGoOnline() {
    return offsetLagThresholdToGoOnline;
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

  public HybridStoreConfig clone(){
    return new HybridStoreConfig(rewindTimeInSeconds, offsetLagThresholdToGoOnline);
  }
}
