package com.linkedin.venice.controller;

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

  public static Map<String, Long> generateMetadataMap(long offset, long executionId, boolean remoteConsumptionEnabled) {
    Map<String, Long> metadata = new HashMap<>();
    metadata.put(remoteConsumptionEnabled ? UPSTREAM_OFFSET_KEY : OFFSET_KEY, offset);
    metadata.put(EXECUTION_ID_KEY, executionId);
    return metadata;
  }

  public static long getOffset(Map<String, Long> metadata, boolean remoteConsumptionEnabled) {
    return metadata.getOrDefault(remoteConsumptionEnabled ? UPSTREAM_OFFSET_KEY : OFFSET_KEY, UNDEFINED_VALUE);
  }

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
