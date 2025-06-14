package com.linkedin.venice.client.store;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Implementation of {@link ComputeAggregationResponse} that handles the results of count-by-value aggregations.
 */
public class AvroComputeAggregationResponse<K> implements ComputeAggregationResponse {
  private final Map<K, ComputeGenericRecord> computeResults;
  private final Map<String, Integer> fieldTopKMap;

  public AvroComputeAggregationResponse(
      Map<K, ComputeGenericRecord> computeResults,
      Map<String, Integer> fieldTopKMap) {
    this.computeResults = computeResults;
    this.fieldTopKMap = fieldTopKMap;
  }

  @Override
  public <T> Map<T, Integer> getValueToCount(String fieldName) {
    if (!fieldTopKMap.containsKey(fieldName)) {
      throw new IllegalArgumentException("No count-by-value aggregation was requested for field: " + fieldName);
    }

    Map<T, Integer> valueToCount = new HashMap<>();
    int topK = fieldTopKMap.get(fieldName);

    // Aggregate counts from all compute results
    for (ComputeGenericRecord record: computeResults.values()) {
      String countFieldName = fieldName + "_count";
      Integer count = (Integer) record.get(countFieldName);
      @SuppressWarnings("unchecked")
      T value = (T) record.get(fieldName);

      // Handle null counts as 0
      if (count == null) {
        count = 0;
      }

      valueToCount.merge(value, count, Integer::sum);
    }

    // Sort by count in descending order and take top K
    return valueToCount.entrySet()
        .stream()
        .sorted(Map.Entry.<T, Integer>comparingByValue().reversed())
        .limit(topK)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
  }

  @Override
  public Map<String, Integer> getBucketNameToCount(String fieldName) {
    throw new UnsupportedOperationException("getBucketNameToCount is not implemented");
  }
}
