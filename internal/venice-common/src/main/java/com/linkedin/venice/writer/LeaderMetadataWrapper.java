package com.linkedin.venice.writer;

/**
 * This is a simple container class to hold Leader metadata related fields together to be passed on to the Put and Delete api in VeniceWriter
 * Caller should construct an instance of this object by properly filling up all the fields of this object.
 */

public class LeaderMetadataWrapper {
  private long upstreamOffset;
  private int upstreamKafkaClusterId;

  public LeaderMetadataWrapper(long upstreamOffset, int upstreamKafkaClusterId) {
    this.upstreamOffset = upstreamOffset;
    this.upstreamKafkaClusterId = upstreamKafkaClusterId;
  }

  public long getUpstreamOffset() {
    return upstreamOffset;
  }

  public int getUpstreamKafkaClusterId() {
    return upstreamKafkaClusterId;
  }
}
