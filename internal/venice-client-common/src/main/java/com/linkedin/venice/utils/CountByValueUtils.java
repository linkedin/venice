package com.linkedin.venice.utils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;


/**
 * Utility class for CountByValue aggregation operations.
 * This class provides shared implementation for processing values and counting field occurrences
 * that can be used by both thin-client and fast-client components.
 */
public class CountByValueUtils {
  private CountByValueUtils() {
    // Utility class
  }

  /**
   * Convert Utf8 objects to String to ensure consistent behavior.
   * This method is shared by both thin-client and fast-client for handling Avro Utf8 values.
   * 
   * @param value The value to convert
   * @return String representation if value is Utf8, otherwise returns the value as-is
   */
  public static Object normalizeValue(Object value) {
    if (value instanceof Utf8) {
      return value.toString();
    }
    return value;
  }

  /**
   * Generic method to aggregate field values with counting.
   * Used by thin-client for local aggregation of compute results.
   * 
   * @param <T> The type of the field values
   * @param values Iterable collection of values to count
   * @return Map of values to their counts
   */
  public static <T> Map<T, Integer> aggregateValues(Iterable<T> values) {
    Map<T, Integer> counts = new HashMap<>();
    for (T value: values) {
      counts.merge(value, 1, Integer::sum);
    }
    return counts;
  }

  /**
   * Generic version of filterTopKValues that works with any comparable key type.
   * This method is shared by both thin-client and fast-client implementations.
   * 
   * @param <T> The type of the field values (must be Comparable)
   * @param fieldCounts Map of field values to counts
   * @param topK Number of top values to keep
   * @return Filtered map with at most topK entries, sorted by count descending
   */
  public static <T extends Comparable<T>> Map<T, Integer> filterTopKValuesGeneric(
      Map<T, Integer> fieldCounts,
      int topK) {
    if (fieldCounts.size() <= topK) {
      return new LinkedHashMap<>(fieldCounts);
    }

    // Sort by count descending, then by key for deterministic results
    return fieldCounts.entrySet().stream().sorted((e1, e2) -> {
      int countCompare = e2.getValue().compareTo(e1.getValue());
      if (countCompare != 0) {
        return countCompare;
      }
      // Handle null keys safely
      if (e1.getKey() == null && e2.getKey() == null) {
        return 0;
      }
      if (e1.getKey() == null) {
        return 1; // null comes last
      }
      if (e2.getKey() == null) {
        return -1; // null comes last
      }
      return e1.getKey().compareTo(e2.getKey());
    }).limit(topK).collect(LinkedHashMap::new, (map, entry) -> map.put(entry.getKey(), entry.getValue()), Map::putAll);
  }

  /**
   * Extract value to count mapping from a collection of records.
   * This method handles both single partition data (server-side) and entire store data (client-side).
   * 
   * @param records Iterable collection of records (can be GenericRecord, String, or any object)
   * @param fieldNames List of field names to extract and count
   * @param valueExtractor Function to extract field value from a record
   * @return Map from field name to (value -> count) mapping
   */
  public static <R> Map<String, Map<Object, Integer>> extractFieldToValueCounts(
      Iterable<R> records,
      List<String> fieldNames,
      FieldValueExtractor<R> valueExtractor) {

    Map<String, Map<Object, Integer>> fieldToValueCounts = new HashMap<>();
    for (String fieldName: fieldNames) {
      fieldToValueCounts.put(fieldName, new HashMap<>());
    }

    for (R record: records) {
      if (record == null) {
        continue;
      }

      for (String fieldName: fieldNames) {
        Object fieldValue = valueExtractor.extractFieldValue(record, fieldName);
        // Count both null and non-null values
        Object normalizedValue;
        if (fieldValue == null) {
          normalizedValue = null;
        } else {
          normalizedValue = normalizeValue(fieldValue);
        }
        fieldToValueCounts.get(fieldName).merge(normalizedValue, 1, Integer::sum);
      }
    }

    return fieldToValueCounts;
  }

  /**
   * Apply TopK filtering to field-to-value-counts mapping.
   * 
   * @param fieldToValueCounts Map from field name to (value -> count) mapping  
   * @param topK Number of top values to keep for each field
   * @return Filtered map with at most topK values per field, sorted by count descending
   */
  public static Map<String, Map<Object, Integer>> applyTopKToFieldCounts(
      Map<String, Map<Object, Integer>> fieldToValueCounts,
      int topK) {

    Map<String, Map<Object, Integer>> result = new HashMap<>();

    for (Map.Entry<String, Map<Object, Integer>> entry: fieldToValueCounts.entrySet()) {
      String fieldName = entry.getKey();
      Map<Object, Integer> valueCounts = entry.getValue();

      if (valueCounts.isEmpty()) {
        result.put(fieldName, new HashMap<>());
      } else {
        // Convert to a comparable map for filtering, then convert back
        Map<String, Integer> stringKeyCounts = new HashMap<>();
        Map<String, Object> keyMapping = new HashMap<>();

        for (Map.Entry<Object, Integer> valueEntry: valueCounts.entrySet()) {
          Object key = valueEntry.getKey();
          String stringKey = (key == null) ? "null" : key.toString();
          stringKeyCounts.put(stringKey, valueEntry.getValue());
          keyMapping.put(stringKey, key);
        }

        Map<String, Integer> topKStringValues = filterTopKValuesGeneric(stringKeyCounts, topK);

        // Convert back to Object keys
        Map<Object, Integer> topKValues = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> stringEntry: topKStringValues.entrySet()) {
          Object originalKey = keyMapping.get(stringEntry.getKey());
          topKValues.put(originalKey, stringEntry.getValue());
        }

        result.put(fieldName, topKValues);
      }
    }

    return result;
  }

  /**
   * Functional interface for extracting field values from different record types.
   * This allows the shared logic to work with GenericRecord, ComputeGenericRecord, or any other type.
   */
  @FunctionalInterface
  public interface FieldValueExtractor<R> {
    /**
     * Extract the value of the specified field from the given record.
     * 
     * @param record The record to extract from
     * @param fieldName The field name to extract
     * @return The field value, or null if not found/not applicable
     */
    Object extractFieldValue(R record, String fieldName);
  }

  /**
   * Generic field value extractor that handles both string-valued and record-valued stores.
   * This method can be used by both server and client implementations.
   * 
   * @param value The value object (can be String, Utf8, or GenericRecord)
   * @param fieldName The field name to extract
   * @return The normalized field value, or null if not found
   */
  public static Object extractFieldValueGeneric(Object value, String fieldName) {
    if (value == null) {
      return null;
    }

    // For string-valued stores, when the field name is "value" or "_value",
    // the entire deserialized value IS the field value we want
    if ((value instanceof String || value instanceof Utf8)
        && ("value".equals(fieldName) || "_value".equals(fieldName))) {
      return normalizeValue(value);
    }

    // For ComputeGenericRecord (thin-client), extract directly using reflection
    if (hasGetMethod(value)) {
      try {
        Object fieldValue = value.getClass().getMethod("get", String.class).invoke(value, fieldName);
        if (fieldValue != null) {
          return normalizeValue(fieldValue);
        }
      } catch (Exception e) {
        // Fall back to GenericRecord check
      }
    }

    // For record-valued stores, extract the specific field
    if (value instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) value;
      // Check if field exists in schema before trying to get it
      Schema schema = record.getSchema();
      if (schema != null && schema.getField(fieldName) != null) {
        Object fieldValue = record.get(fieldName);
        if (fieldValue != null) {
          return normalizeValue(fieldValue);
        }
      }
    }

    return null;
  }

  /**
   * Check if an object has a get method (like ComputeGenericRecord).
   */
  private static boolean hasGetMethod(Object obj) {
    try {
      obj.getClass().getMethod("get", String.class);
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }

  // Removed unused methods - both clients now use the shared generic versions

  /**
   * Validate field names exist in schema (generic version for both clients).
   * 
   * @param fieldNames Array of field names to validate
   * @param valueSchema The schema to validate against
   * @throws IllegalArgumentException if validation fails
   */
  public static void validateFieldNames(String[] fieldNames, Schema valueSchema) {
    // First do basic validation
    if (fieldNames == null || fieldNames.length == 0) {
      throw new IllegalArgumentException("fieldNames cannot be null or empty");
    }

    for (String fieldName: fieldNames) {
      if (fieldName == null || fieldName.isEmpty()) {
        throw new IllegalArgumentException("Field name cannot be null or empty");
      }
    }

    // Then do schema-specific validation
    for (String fieldName: fieldNames) {
      // For string schema, only "value" or "_value" are valid
      if (valueSchema.getType() == Schema.Type.STRING) {
        if (!fieldName.equals("value") && !fieldName.equals("_value")) {
          throw new IllegalArgumentException(
              "For string-valued stores, only 'value' or '_value' field names are supported. Got: " + fieldName);
        }
      } else if (valueSchema.getType() == Schema.Type.RECORD) {
        // For record types, validate field exists
        Schema.Field field = valueSchema.getField(fieldName);
        if (field == null) {
          throw new IllegalArgumentException("Field not found in schema: " + fieldName);
        }
      } else {
        throw new IllegalArgumentException(
            "CountByValue only supports STRING and RECORD value types. Got: " + valueSchema.getType());
      }
    }
  }
}
