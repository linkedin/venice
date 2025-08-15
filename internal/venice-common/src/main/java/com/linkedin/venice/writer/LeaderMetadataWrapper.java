package com.linkedin.venice.writer;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import java.util.Map;
import java.util.Set;


/**
 * This is a simple container class to hold Leader metadata related fields together to be passed on to the Put and Delete api in VeniceWriter
 * Caller should construct an instance of this object by properly filling up all the fields of this object.
 */

public class LeaderMetadataWrapper {
  private final PubSubPosition upstreamPosition;
  private final int upstreamKafkaClusterId;
  private final Map<String, Set<Integer>> viewPartitionMap;
  private final long termId;

  public LeaderMetadataWrapper(PubSubPosition upstreamPosition, int upstreamKafkaClusterId, long termId) {
    this(upstreamPosition, upstreamKafkaClusterId, termId, null);
  }

  public LeaderMetadataWrapper(
      PubSubPosition upstreamPosition,
      int upstreamKafkaClusterId,
      long termId,
      Map<String, Set<Integer>> viewPartitionMap) {
    this.upstreamPosition = upstreamPosition;
    this.upstreamKafkaClusterId = upstreamKafkaClusterId;
    this.termId = termId;
    this.viewPartitionMap = viewPartitionMap;
  }

  public PubSubPosition getUpstreamPosition() {
    return upstreamPosition;
  }

  public int getUpstreamKafkaClusterId() {
    return upstreamKafkaClusterId;
  }

  public Map<String, Set<Integer>> getViewPartitionMap() {
    return viewPartitionMap;
  }

  public long getTermId() {
    return termId;
  }
}
