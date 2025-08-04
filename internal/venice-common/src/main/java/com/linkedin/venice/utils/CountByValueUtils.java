package com.linkedin.venice.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;


/**
 * Utility class for CountByValue aggregation operations.
 * This class provides shared implementation for processing values and counting field occurrences
 * that can be used by both server-side and client-side components.
 */
public class CountByValueUtils {
  private CountByValueUtils() {
    // Utility class
  }

  /**
   * Process a collection of values and count occurrences of specified fields.
   * 
   * @param values Collection of values to process (can be GenericRecord, String, Utf8, etc.)
   * @param fieldNames List of field names to extract and count
   * @return Map where key is field name and value is a map of field values to counts
   */
  public static Map<String, Map<String, Integer>> countFieldOccurrences(
      Iterable<Object> values,
      List<String> fieldNames) {
    Map<String, Map<String, Integer>> fieldCounts = new HashMap<>();

    // Initialize field counts
    for (String fieldName: fieldNames) {
      fieldCounts.put(fieldName, new HashMap<>());
    }

    // Process each value
    for (Object value: values) {
      if (value == null) {
        continue;
      }

      for (String fieldName: fieldNames) {
        String fieldValue = extractFieldValue(value, fieldName);
        if (fieldValue != null) {
          fieldCounts.get(fieldName).merge(fieldValue, 1, Integer::sum);
        }
      }
    }

    return fieldCounts;
  }

  /**
   * Process a set of keys and their corresponding values, counting occurrences of specified fields.
   * 
   * @param keyValuePairs Map of keys to values
   * @param fieldNames List of field names to extract and count
   * @return Map where key is field name and value is a map of field values to counts
   */
  public static Map<String, Map<String, Integer>> countFieldOccurrences(
      Map<?, Object> keyValuePairs,
      List<String> fieldNames) {
    return countFieldOccurrences(keyValuePairs.values(), fieldNames);
  }

  /**
   * Extract field value from a value object.
   * Supports String values (for string-valued stores) and GenericRecord (for record-valued stores).
   * 
   * @param value The value object (String, Utf8, GenericRecord, etc.)
   * @param fieldName The field name to extract
   * @return The extracted field value as a string, or null if not found
   */
  public static String extractFieldValue(Object value, String fieldName) {
    if (value == null) {
      return null;
    }

    // For string-valued stores, when the field name is "value" or "_value",
    // the entire deserialized value IS the field value we want
    if ((value instanceof String || value instanceof Utf8)
        && ("value".equals(fieldName) || "_value".equals(fieldName))) {
      return value instanceof Utf8 ? ((Utf8) value).toString() : (String) value;
    }

    // For record-valued stores, extract the specific field
    if (value instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) value;
      Object fieldValue = record.get(fieldName);
      if (fieldValue != null) {
        if (fieldValue instanceof Utf8) {
          return ((Utf8) fieldValue).toString();
        } else {
          return fieldValue.toString();
        }
      }
    }

    return null;
  }

  /**
   * Add a single value occurrence to the field counts map.
   * 
   * @param fieldCounts The map to update
   * @param value The value to process
   * @param fieldNames The field names to extract and count
   */
  public static void addValueToFieldCounts(
      Map<String, Map<String, Integer>> fieldCounts,
      Object value,
      List<String> fieldNames) {
    if (value == null || fieldCounts == null) {
      return;
    }

    for (String fieldName: fieldNames) {
      String fieldValue = extractFieldValue(value, fieldName);
      if (fieldValue != null) {
        fieldCounts.get(fieldName).merge(fieldValue, 1, Integer::sum);
      }
    }
  }

  /**
   * Filter top K values from a field count map.
   * 
   * @param fieldCounts Map of field values to counts
   * @param topK Number of top values to keep
   * @return Filtered map with at most topK entries
   */
  public static Map<String, Integer> filterTopKValues(Map<String, Integer> fieldCounts, int topK) {
    if (fieldCounts.size() <= topK) {
      return fieldCounts;
    }

    // Sort by count descending, then by key for deterministic results
    return fieldCounts.entrySet().stream().sorted((e1, e2) -> {
      int countCompare = e2.getValue().compareTo(e1.getValue());
      if (countCompare != 0) {
        return countCompare;
      }
      return e1.getKey().compareTo(e2.getKey());
    }).limit(topK).collect(HashMap::new, (map, entry) -> map.put(entry.getKey(), entry.getValue()), HashMap::putAll);
  }
}
