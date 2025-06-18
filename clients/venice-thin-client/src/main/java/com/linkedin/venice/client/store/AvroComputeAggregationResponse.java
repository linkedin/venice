package com.linkedin.venice.client.store;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;


/**
 * Implementation of {@link ComputeAggregationResponse} that handles the results of count-by-value aggregations.
 */
public class AvroComputeAggregationResponse<K> implements ComputeAggregationResponse {
  private final Map<K, ComputeGenericRecord> computeResults;
  private final Map<String, Integer> fieldTopKMap;
  private final Schema valueSchema;

  public AvroComputeAggregationResponse(
      Map<K, ComputeGenericRecord> computeResults,
      Map<String, Integer> fieldTopKMap) {
    this.computeResults = computeResults;
    this.fieldTopKMap = fieldTopKMap;
    // Get the value schema from the first non-null record
    this.valueSchema = computeResults.values()
        .stream()
        .filter(record -> record != null)
        .findFirst()
        .map(record -> record.getSchema())
        .orElse(null);
  }

  @Override
  public Map<String, Integer> getValueToCount(String field) {
    Map<String, Integer> valueToCount = new HashMap<>();

    for (ComputeGenericRecord record: computeResults.values()) {
      Object value = record.get(field);
      if (value instanceof Utf8) {
        value = value.toString();
      }
      String key = (value == null) ? null : value.toString();
      valueToCount.merge(key, 1, Integer::sum);
    }

    // Sort by count in descending order
    Map<String, Integer> sortedMap = new LinkedHashMap<>();
    valueToCount.entrySet()
        .stream()
        .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
        .limit(fieldTopKMap.getOrDefault(field, Integer.MAX_VALUE))
        .forEach(entry -> sortedMap.put(entry.getKey(), entry.getValue()));

    return sortedMap;
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
