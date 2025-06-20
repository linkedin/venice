package com.linkedin.venice.client.store;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.avro.util.Utf8;


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
  public <T> Map<T, Integer> getValueToCount(String field) {
    // Quick check: if field doesn't exist in fieldTopKMap, return empty map
    if (!fieldTopKMap.containsKey(field)) {
      return new LinkedHashMap<>();
    }

    Map<T, Integer> valueToCount = new HashMap<>();

    for (ComputeGenericRecord record: computeResults.values()) {
      Object value = record.get(field);
      if (value instanceof Utf8) {
        value = value.toString();
      }

      @SuppressWarnings("unchecked")
      T key = (T) value;
      valueToCount.merge(key, 1, Integer::sum);
    }

    // Sort by count in descending order
    Map<T, Integer> sortedMap = new LinkedHashMap<>();
    valueToCount.entrySet()
        .stream()
        .sorted(Map.Entry.<T, Integer>comparingByValue().reversed())
        .limit(fieldTopKMap.get(field))
        .forEach(entry -> sortedMap.put(entry.getKey(), entry.getValue()));

    return sortedMap;
  }

  @Override
  public Map<String, Integer> getBucketNameToCount(String fieldName) {
    throw new UnsupportedOperationException("getBucketNameToCount is not implemented");
  }
}
