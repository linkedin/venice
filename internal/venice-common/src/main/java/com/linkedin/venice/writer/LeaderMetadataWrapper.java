package com.linkedin.venice.writer;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import java.util.Map;
import java.util.Set;


/**
 * This is a simple container class to hold Leader metadata related fields together to be passed on to the Put and Delete api in VeniceWriter
 * Caller should construct an instance of this object by properly filling up all the fields of this object.
 */

public class LeaderMetadataWrapper {
  /**
   * Sentinel value indicating that no upstream message timestamp is available for the produced record
   * (e.g., self-generated records that do not originate from a consumed upstream message).
   */
  public static final long DEFAULT_UPSTREAM_MESSAGE_TIMESTAMP = -1L;

  private final PubSubPosition upstreamPosition;
  private final int upstreamKafkaClusterId;
  private final Map<String, Set<Integer>> viewPartitionMap;
  private final long termId;
  private final long upstreamMessageTimestamp;

  /**
   * Backward-compatible 3-arg constructor for call sites that don't carry an upstream message
   * timestamp or a view-partition map. Defaults both to their sentinel/null.
   */
  public LeaderMetadataWrapper(PubSubPosition upstreamPosition, int upstreamKafkaClusterId, long termId) {
    this(upstreamPosition, upstreamKafkaClusterId, termId, DEFAULT_UPSTREAM_MESSAGE_TIMESTAMP, null);
  }

  /**
   * Canonical constructor. Pass {@link #DEFAULT_UPSTREAM_MESSAGE_TIMESTAMP} when no upstream
   * timestamp is available, and {@code null} when no view-partition map is needed.
   */
  public LeaderMetadataWrapper(
      PubSubPosition upstreamPosition,
      int upstreamKafkaClusterId,
      long termId,
      long upstreamMessageTimestamp,
      Map<String, Set<Integer>> viewPartitionMap) {
    this.upstreamPosition = upstreamPosition;
    this.upstreamKafkaClusterId = upstreamKafkaClusterId;
    this.termId = termId;
    this.upstreamMessageTimestamp = upstreamMessageTimestamp;
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

  /**
   * The timestamp of the upstream message from which the leader produced this record, expressed
   * as milliseconds since the Unix epoch (matching the time basis of
   * {@code ProducerMetadata.messageTimestamp}). Returns {@link #DEFAULT_UPSTREAM_MESSAGE_TIMESTAMP}
   * when no upstream message is available.
   */
  public long getUpstreamMessageTimestamp() {
    return upstreamMessageTimestamp;
  }
}
