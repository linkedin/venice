package com.linkedin.venice.writer;

import java.nio.ByteBuffer;
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
  private final ByteBuffer upstreamPubSubPosition;
  private final long termId;

  public LeaderMetadataWrapper(
      long upstreamOffset,
      int upstreamKafkaClusterId,
      long termId,
      ByteBuffer upstreamPubSubPosition) {
    this(upstreamOffset, upstreamKafkaClusterId, termId, upstreamPubSubPosition, null);
  }

  public LeaderMetadataWrapper(
      long upstreamOffset,
      int upstreamKafkaClusterId,
      long termId,
      ByteBuffer upstreamPubSubPosition,
      Map<String, Set<Integer>> viewPartitionMap) {
    this.upstreamOffset = upstreamOffset;
    this.upstreamKafkaClusterId = upstreamKafkaClusterId;
    this.termId = termId;
    this.upstreamPubSubPosition = upstreamPubSubPosition;
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

  public ByteBuffer getUpstreamPubSubPosition() {
    return upstreamPubSubPosition;
  }

  public long getTermId() {
    return termId;
  }
}
