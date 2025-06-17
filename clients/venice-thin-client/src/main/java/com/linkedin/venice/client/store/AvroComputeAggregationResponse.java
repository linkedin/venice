package com.linkedin.venice.client.store;

import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.LongPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;


/**
 * Implementation of {@link ComputeAggregationResponse} that handles the results of count-by-value aggregations.
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
    Map<String, Predicate> buckets = fieldBucketMap.get(fieldName);
    if (buckets == null || buckets.isEmpty()) {
      throw new IllegalArgumentException("No count-by-bucket aggregation was requested for field: " + fieldName);
    }

    // Initialize bucket counts
    Map<String, Integer> bucketCounts = new LinkedHashMap<>();
    for (String bucketName: buckets.keySet()) {
      bucketCounts.put(bucketName, 0);
    }

    // Process all records
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
}
