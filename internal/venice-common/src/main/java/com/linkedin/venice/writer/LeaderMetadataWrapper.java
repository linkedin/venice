package com.linkedin.venice.writer;

import java.util.Map;
import java.util.Set;


/**
 * This is a simple container class to hold Leader metadata related fields together to be passed on to the Put and Delete api in VeniceWriter
 * Caller should construct an instance of this object by properly filling up all the fields of this object.
 */

public class LeaderMetadataWrapper {
  private final long upstreamOffset;
  private final int upstreamKafkaClusterId;
  private final Map<String, Set<Integer>> viewPartitionMap;

  public LeaderMetadataWrapper(long upstreamOffset, int upstreamKafkaClusterId) {
    this(upstreamOffset, upstreamKafkaClusterId, null);
  }

  public LeaderMetadataWrapper(
      long upstreamOffset,
      int upstreamKafkaClusterId,
      Map<String, Set<Integer>> viewPartitionMap) {
    this.upstreamOffset = upstreamOffset;
    this.upstreamKafkaClusterId = upstreamKafkaClusterId;
    this.viewPartitionMap = viewPartitionMap;
  }

  public long getUpstreamOffset() {
    return upstreamOffset;
  }

  public int getUpstreamKafkaClusterId() {
    return upstreamKafkaClusterId;
  }

  public Map<String, Set<Integer>> getViewPartitionMap() {
    return viewPartitionMap;
  }
}
