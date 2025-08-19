package com.linkedin.venice.client.store;

import com.linkedin.venice.client.store.predicate.Predicate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Implementation of {@link ComputeAggregationResponse} that handles the results of count-by-value and count-by-bucket aggregations.
 */
public class AvroComputeAggregationResponse<K> implements ComputeAggregationResponse {
  private final Map<K, ComputeGenericRecord> computeResults;
  private final Map<String, Integer> fieldTopKMap;
  private final Map<String, Map<String, Predicate>> fieldBucketMap;

  public AvroComputeAggregationResponse(
      Map<K, ComputeGenericRecord> computeResults,
      Map<String, Integer> fieldTopKMap) {
    this(computeResults, fieldTopKMap, new HashMap<>());
  }

  public AvroComputeAggregationResponse(
      Map<K, ComputeGenericRecord> computeResults,
      Map<String, Integer> fieldTopKMap,
      Map<String, Map<String, Predicate>> fieldBucketMap) {
    this.computeResults = computeResults;
    this.fieldTopKMap = fieldTopKMap;
    this.fieldBucketMap = fieldBucketMap;
  }

  @Override
  public <T> Map<T, Integer> getValueToCount(String field) {
    // Quick check: if field doesn't exist in fieldTopKMap, return empty map
    if (!fieldTopKMap.containsKey(field)) {
      return Collections.emptyMap();
    }

    // Use utility method - original logic moved to FacetCountingUtils
    int topK = fieldTopKMap.get(field);
    return FacetCountingUtils.getValueToCount(computeResults.values(), field, topK);
  }

  @Override
  public Map<String, Integer> getBucketNameToCount(String fieldName) {
    // Quick check: if field doesn't exist in fieldBucketMap, throw exception
    Map<String, Predicate> buckets = fieldBucketMap.get(fieldName);
    if (buckets == null || buckets.isEmpty()) {
      throw new IllegalArgumentException("No count-by-bucket aggregation was requested for field: " + fieldName);
    }

    // Use utility method - original logic moved to FacetCountingUtils
    return FacetCountingUtils.getBucketNameToCount(computeResults.values(), fieldName, buckets);
  }

}
