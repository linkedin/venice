package com.linkedin.venice.client.store;

import com.linkedin.venice.client.store.predicate.DoublePredicate;
import com.linkedin.venice.client.store.predicate.FloatPredicate;
import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.LongPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;


/**
 * This class provides shared implementation for processing values and counting field occurrences
 * that can be used by thin-client, fast-client, and server components.
 */
public class FacetCountingUtils {
  /**
   * Get value counts for a single field with TopK filtering.
   * This is the original logic from AvroComputeAggregationResponse.getValueToCount() moved to Utils.
   * 
   * @param records Collection of GenericRecord results (supports both ComputeGenericRecord and server GenericRecord)
   * @param field The field name to count
   * @param topK Maximum number of top values to return  
   * @return Map of values to their counts, limited to topK entries
   */
  public static <T> Map<T, Integer> getValueToCount(Iterable<? extends GenericRecord> records, String field, int topK) {

    Map<T, Integer> valueToCount = new HashMap<>();

    for (GenericRecord record: records) {
      Object value = record.get(field);
      // Convert Utf8 to String for consistent behavior
      if (value instanceof Utf8) {
        value = value.toString();
      }
      @SuppressWarnings("unchecked")
      T key = (T) value;
      valueToCount.merge(key, 1, Integer::sum);
    }

    // Sort by count in descending order
    Map<T, Integer> sortedMap = valueToCount.entrySet()
        .stream()
        .sorted(Map.Entry.<T, Integer>comparingByValue().reversed())
        .limit(topK)
        .collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);

    return sortedMap;
  }

  /**
   * Get bucket counts for a single field with predicate-based bucketing.
   * This is the original logic from AvroComputeAggregationResponse.getBucketNameToCount() moved to Utils.
   * 
   * @param records Collection of GenericRecord results (supports both ComputeGenericRecord and server GenericRecord)
   * @param fieldName The field name to count
   * @param buckets Map of bucket names to their predicates
   * @return Map of bucket names to their counts
   */
  public static Map<String, Integer> getBucketNameToCount(
      Iterable<? extends GenericRecord> records,
      String fieldName,
      Map<String, Predicate> buckets) {

    // Initialize bucket counts
    Map<String, Integer> bucketCounts = new LinkedHashMap<>();
    for (String bucketName: buckets.keySet()) {
      bucketCounts.put(bucketName, 0);
    }

    // Process all records and count bucket matches
    for (GenericRecord record: records) {
      if (record == null) {
        continue;
      }

      Object fieldValue = record.get(fieldName);
      if (fieldValue == null) {
        continue;
      }

      // Convert field value if needed (Utf8 to String)
      Object convertedValue = fieldValue;
      if (fieldValue instanceof Utf8) {
        convertedValue = fieldValue.toString();
      }

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
   * Generic method to convert value to the target type for predicate evaluation.
   * Supports Integer, Long, Float, and Double conversions.
   */
  @SuppressWarnings("unchecked")
  public static <T> T convertToType(Object value, Class<T> targetType) {
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

  /**
   * Validate field names exist in schema (generic version for both clients).
   * 
   * @param fieldNames Array of field names to validate
   * @param valueSchema The schema to validate against
   * @throws IllegalArgumentException if validation fails
   */
  public static void validateFieldNames(String[] fieldNames, Schema valueSchema) {
    if (fieldNames == null || fieldNames.length == 0) {
      throw new IllegalArgumentException("fieldNames cannot be null or empty");
    }

    for (String fieldName: fieldNames) {
      if (fieldName == null) {
        throw new IllegalArgumentException("Field name cannot be null");
      }
      if (fieldName.isEmpty()) {
        throw new IllegalArgumentException("Field name cannot be empty");
      }

      Schema.Field field = valueSchema.getField(fieldName);
      if (field == null) {
        throw new IllegalArgumentException("Field not found in schema: " + fieldName);
      }
    }
  }
}
