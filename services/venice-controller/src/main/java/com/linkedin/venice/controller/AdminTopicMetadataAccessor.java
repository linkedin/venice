package com.linkedin.venice.controller;

import com.linkedin.venice.utils.Pair;
import java.util.HashMap;
import java.util.Map;


public abstract class AdminTopicMetadataAccessor {
  private static final String OFFSET_KEY = "offset";
  /**
   * When remote consumption is enabled, child controller will consume directly from the source admin topic; an extra
   * metadata called upstream offset will be maintained, which indicate the last offset in the source admin topic that
   * gets processed successfully.
   */
  private static final String UPSTREAM_OFFSET_KEY = "upstreamOffset";
  private static final String EXECUTION_ID_KEY = "executionId";
  private static final long UNDEFINED_VALUE = -1;

  /**
   * @return a map with {@linkplain AdminTopicMetadataAccessor#OFFSET_KEY}, {@linkplain AdminTopicMetadataAccessor#UPSTREAM_OFFSET_KEY},
   *         {@linkplain AdminTopicMetadataAccessor#EXECUTION_ID_KEY} specified to input values.
   */
  public static Map<String, Long> generateMetadataMap(long localOffset, long upstreamOffset, long executionId) {
    Map<String, Long> metadata = new HashMap<>();
    metadata.put(OFFSET_KEY, localOffset);
    metadata.put(UPSTREAM_OFFSET_KEY, upstreamOffset);
    metadata.put(EXECUTION_ID_KEY, executionId);
    return metadata;
  }

  /**
   * @return a pair of values to which the specified keys are mapped to {@linkplain AdminTopicMetadataAccessor#OFFSET_KEY}
   * and {@linkplain AdminTopicMetadataAccessor#UPSTREAM_OFFSET_KEY}.
   */
  public static Pair<Long, Long> getOffsets(Map<String, Long> metadata) {
    long localOffset = metadata.getOrDefault(OFFSET_KEY, UNDEFINED_VALUE);
    long upstreamOffset = metadata.getOrDefault(UPSTREAM_OFFSET_KEY, UNDEFINED_VALUE);
    return new Pair<>(localOffset, upstreamOffset);
  }

  /**
   * @return the value to which the specified key is mapped to {@linkplain AdminTopicMetadataAccessor#EXECUTION_ID_KEY}.
   */
  public static long getExecutionId(Map<String, Long> metadata) {
    return metadata.getOrDefault(EXECUTION_ID_KEY, UNDEFINED_VALUE);
  }

  /**
   * Update all relevant metadata for a given cluster in a single transaction.
   * @param clusterName of the cluster at interest.
   * @param metadata map containing relevant information.
   */
  public abstract void updateMetadata(String clusterName, Map<String, Long> metadata);

  /**
   * Retrieve the latest metadata map.
   * @param clusterName of the cluster at interest.
   */
  public abstract Map<String, Long> getMetadata(String clusterName);
}
