package com.linkedin.venice.client.store;

import com.linkedin.venice.client.store.predicate.DoublePredicate;
import com.linkedin.venice.client.store.predicate.FloatPredicate;
import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.LongPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;


/**
 * Implementation of {@link ComputeAggregationResponse} that handles the results of count-by-value and count-by-bucket aggregations.
 */
public class AvroComputeAggregationResponse<K> implements ComputeAggregationResponse {
  private final Map<K, ? extends GenericRecord> computeResults;
  private final Map<String, Integer> fieldTopKMap;
  private final Map<String, Map<String, Predicate>> fieldBucketMap;

  public AvroComputeAggregationResponse(
      Map<K, ? extends GenericRecord> computeResults,
      Map<String, Integer> fieldTopKMap) {
    this(computeResults, fieldTopKMap, new HashMap<>());
  }

  public AvroComputeAggregationResponse(
      Map<K, ? extends GenericRecord> computeResults,
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

    for (GenericRecord record: computeResults.values()) {
      Object value = record.get(field);
      if (value instanceof Utf8) {
        value = value.toString();
      }
      @SuppressWarnings("unchecked")
      T key = (T) value;
      valueToCount.merge(key, 1, Integer::sum);
    }

    // Sort by count in descending order and limit to topK
    List<Map.Entry<T, Integer>> sortedEntries = new ArrayList<>(valueToCount.entrySet());
    sortedEntries.sort(Map.Entry.<T, Integer>comparingByValue().reversed());

    int topK = fieldTopKMap.get(field);
    Map<T, Integer> sortedMap = new LinkedHashMap<>();
    for (int i = 0; i < Math.min(topK, sortedEntries.size()); i++) {
      Map.Entry<T, Integer> entry = sortedEntries.get(i);
      sortedMap.put(entry.getKey(), entry.getValue());
    }

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
    for (Map.Entry<K, ? extends GenericRecord> entry: computeResults.entrySet()) {
      GenericRecord record = entry.getValue();

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
          if (predicate instanceof LongPredicate && !(convertedValue instanceof Long)) {
            valueToEvaluate = convertToLong(convertedValue);
          } else if (predicate instanceof IntPredicate && !(convertedValue instanceof Integer)) {
            valueToEvaluate = convertToInteger(convertedValue);
          } else if (predicate instanceof FloatPredicate && !(convertedValue instanceof Float)) {
            valueToEvaluate = convertToFloat(convertedValue);
          } else if (predicate instanceof DoublePredicate && !(convertedValue instanceof Double)) {
            valueToEvaluate = convertToDouble(convertedValue);
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
   * Convert value to Long for LongPredicate evaluation.
   */
  private Long convertToLong(Object value) {
    if (value instanceof Long) {
      return (Long) value;
    } else if (value instanceof Integer) {
      return ((Integer) value).longValue();
    } else if (value instanceof String) {
      try {
        return Long.parseLong((String) value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  /**
   * Convert value to Integer for IntPredicate evaluation.
   */
  private Integer convertToInteger(Object value) {
    if (value instanceof Integer) {
      return (Integer) value;
    } else if (value instanceof Long) {
      return ((Long) value).intValue();
    } else if (value instanceof String) {
      try {
        return Integer.parseInt((String) value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  /**
   * Convert value to Float for FloatPredicate evaluation.
   */
  private Float convertToFloat(Object value) {
    if (value instanceof Float) {
      return (Float) value;
    } else if (value instanceof Integer) {
      return ((Integer) value).floatValue();
    } else if (value instanceof String) {
      try {
        return Float.parseFloat((String) value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  /**
   * Convert value to Double for DoublePredicate evaluation.
   */
  private Double convertToDouble(Object value) {
    if (value instanceof Double) {
      return (Double) value;
    } else if (value instanceof Integer) {
      return ((Integer) value).doubleValue();
    } else if (value instanceof String) {
      try {
        return Double.parseDouble((String) value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }
}
