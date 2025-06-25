package com.linkedin.venice.client.store;

import com.linkedin.venice.client.store.predicate.DoublePredicate;
import com.linkedin.venice.client.store.predicate.FloatPredicate;
import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.LongPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;


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
    // Quick check: if field doesn't exist in fieldBucketMap, throw exception
    Map<String, Predicate> buckets = fieldBucketMap.get(fieldName);
    if (buckets == null || buckets.isEmpty()) {
      throw new IllegalArgumentException("No count-by-bucket aggregation was requested for field: " + fieldName);
    }

    // Initialize bucket counts
    Map<String, Integer> bucketCounts = new LinkedHashMap<>();
    for (String bucketName: buckets.keySet()) {
      bucketCounts.put(bucketName, 0);
    }

    // Process all records and count bucket matches
    for (Map.Entry<K, ComputeGenericRecord> entry: computeResults.entrySet()) {
      ComputeGenericRecord record = entry.getValue();

      if (record == null) {
        continue;
      }

      Object fieldValue = record.get(fieldName);
      if (fieldValue == null) {
        continue;
      }

      // Check which bucket(s) this value falls into
      for (Map.Entry<String, Predicate> bucketEntry: buckets.entrySet()) {
        String bucketName = bucketEntry.getKey();
        Predicate predicate = bucketEntry.getValue();

        try {
          boolean matches = false;

          // Convert field value if needed
          Object convertedValue = convertUtf8ToString(fieldValue);

          // Handle different value types
          if (convertedValue instanceof GenericRecord) {
            matches = predicate.evaluate((GenericRecord) convertedValue);
          } else if (predicate instanceof LongPredicate) {
            // Convert to Long if needed
            Long longValue;
            if (convertedValue instanceof Long) {
              longValue = (Long) convertedValue;
            } else if (convertedValue instanceof Integer) {
              longValue = ((Integer) convertedValue).longValue();
            } else if (convertedValue instanceof String) {
              try {
                longValue = Long.parseLong((String) convertedValue);
              } catch (NumberFormatException e) {
                continue;
              }
            } else {
              continue;
            }
            matches = ((LongPredicate) predicate).evaluate(longValue);
          } else if (predicate instanceof IntPredicate) {
            // Convert to Integer if needed
            Integer intValue;
            if (convertedValue instanceof Integer) {
              intValue = (Integer) convertedValue;
            } else if (convertedValue instanceof Long) {
              intValue = ((Long) convertedValue).intValue();
            } else if (convertedValue instanceof String) {
              try {
                intValue = Integer.parseInt((String) convertedValue);
              } catch (NumberFormatException e) {
                continue;
              }
            } else {
              continue;
            }
            matches = ((IntPredicate) predicate).evaluate(intValue);
          } else if (predicate instanceof FloatPredicate) {
            // Convert to Float if needed
            Float floatValue;
            if (convertedValue instanceof Float) {
              floatValue = (Float) convertedValue;
            } else if (convertedValue instanceof Integer) {
              floatValue = ((Integer) convertedValue).floatValue();
            } else if (convertedValue instanceof String) {
              try {
                floatValue = Float.parseFloat((String) convertedValue);
              } catch (NumberFormatException e) {
                continue;
              }
            } else {
              continue;
            }
            matches = ((FloatPredicate) predicate).evaluate(floatValue);
          } else if (predicate instanceof DoublePredicate) {
            // Convert to Double if needed
            Double doubleValue;
            if (convertedValue instanceof Double) {
              doubleValue = (Double) convertedValue;
            } else if (convertedValue instanceof Integer) {
              doubleValue = ((Integer) convertedValue).doubleValue();
            } else if (convertedValue instanceof String) {
              try {
                doubleValue = Double.parseDouble((String) convertedValue);
              } catch (NumberFormatException e) {
                continue;
              }
            } else {
              continue;
            }
            matches = ((DoublePredicate) predicate).evaluate(doubleValue);
          } else {
            matches = predicate.evaluate(convertedValue);
          }

          if (matches) {
            bucketCounts.merge(bucketName, 1, Integer::sum);
          }
        } catch (ClassCastException e) {
          continue;
        }
      }
    }

    return bucketCounts;
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
}
