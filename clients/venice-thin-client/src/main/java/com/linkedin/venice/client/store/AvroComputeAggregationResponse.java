package com.linkedin.venice.client.store;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
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
  public <T> Map<T, Integer> getValueToCount(String fieldName) {
    if (!fieldTopKMap.containsKey(fieldName)) {
      throw new IllegalArgumentException("No count-by-value aggregation was requested for field: " + fieldName);
    }

    Map<T, Integer> valueToCount = new HashMap<>();
    int topK = fieldTopKMap.get(fieldName);

    // Aggregate counts from all compute results
    for (ComputeGenericRecord record: computeResults.values()) {
      if (record == null) {
        continue;
      }

      Object fieldValue = record.get(fieldName);

      if (fieldValue == null) {
        // Handle null field value
        valueToCount.merge(null, 1, Integer::sum);
      } else if (fieldValue instanceof Collection) {
        // Handle array fields
        Collection<?> collection = (Collection<?>) fieldValue;
        if (collection != null) {
          for (Object value: collection) {
            @SuppressWarnings("unchecked")
            T typedValue = (T) convertUtf8ToString(value);
            valueToCount.merge(typedValue, 1, Integer::sum);
          }
        }
      } else if (fieldValue instanceof Map) {
        // Handle map fields - count the values (not keys)
        Map<?, ?> map = (Map<?, ?>) fieldValue;
        if (map != null) {
          for (Object value: map.values()) {
            @SuppressWarnings("unchecked")
            T typedValue = (T) convertUtf8ToString(value);
            valueToCount.merge(typedValue, 1, Integer::sum);
          }
        }
      } else {
        // Handle scalar fields (shouldn't happen based on validation, but be defensive)
        @SuppressWarnings("unchecked")
        T typedValue = (T) convertUtf8ToString(fieldValue);
        valueToCount.merge(typedValue, 1, Integer::sum);
      }
    }

    // Sort by count in descending order and take top K
    return valueToCount.entrySet()
        .stream()
        .sorted(Map.Entry.<T, Integer>comparingByValue().reversed())
        .limit(topK)
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (e1, e2) -> e1, // If there are duplicate keys, keep the first one
                LinkedHashMap::new));
  }

  /**
   * Convert Utf8 objects to String to ensure consistent behavior between unit tests and integration tests.
   * In integration tests, Avro deserialization produces Utf8 objects for string fields,
   * while unit tests with mocked data use String objects directly.
   */
  private Object convertUtf8ToString(Object value) {
    if (value instanceof Utf8) {
      return value.toString();
    }
    return value;
  }

  @Override
  public Map<String, Integer> getBucketNameToCount(String fieldName) {
    throw new UnsupportedOperationException("getBucketNameToCount is not implemented");
  }
}
