package com.linkedin.venice.client.store;

import com.linkedin.venice.client.store.predicate.DoublePredicate;
import com.linkedin.venice.client.store.predicate.FloatPredicate;
import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.LongPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
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
      return Collections.emptyMap();
    }

    Map<T, Integer> valueToCount = new HashMap<>();

    for (ComputeGenericRecord record: computeResults.values()) {
      Object value = convertUtf8ToString(record.get(field));
      @SuppressWarnings("unchecked")
      T key = (T) value;
      valueToCount.merge(key, 1, Integer::sum);
    }

    // Sort by count in descending order
    Map<T, Integer> sortedMap = valueToCount.entrySet()
        .stream()
        .sorted(Map.Entry.<T, Integer>comparingByValue().reversed())
        .limit(fieldTopKMap.get(field))
        .collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);

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

      // Convert field value if needed (Utf8 to String)
      Object convertedValue = convertUtf8ToString(fieldValue);

      // Check which bucket(s) this value falls into
      for (Map.Entry<String, Predicate> bucketEntry: buckets.entrySet()) {
        String bucketName = bucketEntry.getKey();
        Predicate predicate = bucketEntry.getValue();

        try {
          // Handle type conversion for numeric predicates
          Object valueToEvaluate = convertedValue;
          if (predicate instanceof LongPredicate) {
            valueToEvaluate = convertToType(convertedValue, Long.class);
          } else if (predicate instanceof IntPredicate) {
            valueToEvaluate = convertToType(convertedValue, Integer.class);
          } else if (predicate instanceof FloatPredicate) {
            valueToEvaluate = convertToType(convertedValue, Float.class);
          } else if (predicate instanceof DoublePredicate) {
            valueToEvaluate = convertToType(convertedValue, Double.class);
          }

          if (valueToEvaluate != null) {
            boolean matches = predicate.evaluate(valueToEvaluate);
            if (matches) {
              bucketCounts.merge(bucketName, 1, Integer::sum);
            }
          }
        } catch (ClassCastException | NumberFormatException e) {
          // If type conversion fails, skip this bucket for this record
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

  /**
   * Generic method to convert value to the target type for predicate evaluation.
   * Supports Integer, Long, Float, and Double conversions.
   */
  @SuppressWarnings("unchecked")
  private <T> T convertToType(Object value, Class<T> targetType) {
    if (value == null) {
      return null;
    }

    // If already the target type, return as is
    if (targetType.isInstance(value)) {
      return (T) value;
    }

    // Handle numeric conversions
    if (targetType == Integer.class) {
      if (value instanceof Long) {
        return (T) Integer.valueOf(((Long) value).intValue());
      } else if (value instanceof String) {
        try {
          return (T) Integer.valueOf(Integer.parseInt((String) value));
        } catch (NumberFormatException e) {
          return null;
        }
      }
    } else if (targetType == Long.class) {
      if (value instanceof Integer) {
        return (T) Long.valueOf(((Integer) value).longValue());
      } else if (value instanceof String) {
        try {
          return (T) Long.valueOf(Long.parseLong((String) value));
        } catch (NumberFormatException e) {
          return null;
        }
      }
    } else if (targetType == Float.class) {
      if (value instanceof Integer) {
        return (T) Float.valueOf(((Integer) value).floatValue());
      } else if (value instanceof String) {
        try {
          return (T) Float.valueOf(Float.parseFloat((String) value));
        } catch (NumberFormatException e) {
          return null;
        }
      }
    } else if (targetType == Double.class) {
      if (value instanceof Integer) {
        return (T) Double.valueOf(((Integer) value).doubleValue());
      } else if (value instanceof String) {
        try {
          return (T) Double.valueOf(Double.parseDouble((String) value));
        } catch (NumberFormatException e) {
          return null;
        }
      }
    }

    return null;
  }

}
