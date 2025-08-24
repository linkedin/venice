package com.linkedin.venice.fastclient;

import com.linkedin.venice.read.RequestType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Request context for CountByValue operations in FastClient.
 * This follows the same pattern as BatchGetRequestContext to ensure consistent behavior.
 */
public class CountByValueRequestContext<K> extends MultiKeyRequestContext<K, Map<Object, Integer>> {
  private final String fieldName;
  private final int topK;

  // Aggregated results from all partitions
  private final Map<Object, Integer> aggregatedCounts = new ConcurrentHashMap<>();

  public CountByValueRequestContext(int keySize, String fieldName, int topK) {
    super(keySize, false); // allowPartialSuccess = false for consistency with batchGet
    this.fieldName = fieldName;
    this.topK = topK;
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.COUNT_BY_VALUE;
  }

  public String getFieldName() {
    return fieldName;
  }

  public int getTopK() {
    return topK;
  }

  public Map<Object, Integer> getAggregatedCounts() {
    return aggregatedCounts;
  }

  /**
   * Aggregate counts from a partition into the overall result.
   * This method is thread-safe for concurrent aggregation from multiple routes.
   */
  public synchronized void aggregateCounts(Map<Object, Integer> partitionCounts) {
    for (Map.Entry<Object, Integer> entry: partitionCounts.entrySet()) {
      aggregatedCounts.merge(entry.getKey(), entry.getValue(), Integer::sum);
    }
  }
}
