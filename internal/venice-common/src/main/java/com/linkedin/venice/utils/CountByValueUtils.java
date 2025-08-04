package com.linkedin.venice.utils;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.protocols.CountByValueResponse;
import com.linkedin.venice.protocols.ValueCount;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.AvroStoreDeserializerCache;
import com.linkedin.venice.serializer.RecordDeserializer;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Utility class for CountByValue aggregation operations.
 * This class provides shared implementation for processing values and counting field occurrences
 * that can be used by both server-side and client-side components.
 */
public class CountByValueUtils {
  private static final Logger LOGGER = LogManager.getLogger(CountByValueUtils.class);

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
      // Check if field exists in schema before trying to get it
      if (record.getSchema().getField(fieldName) != null) {
        Object fieldValue = record.get(fieldName);
        if (fieldValue != null) {
          if (fieldValue instanceof Utf8) {
            return ((Utf8) fieldValue).toString();
          } else {
            return fieldValue.toString();
          }
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

  /**
   * Validate that field names are valid for the given schema.
   * 
   * @param fieldNames List of field names to validate
   * @param valueSchema The schema to validate against
   * @param storeName Store name for error messages
   * @return Error response if validation fails, null if validation passes
   */
  public static CountByValueResponse validateFields(List<String> fieldNames, Schema valueSchema, String storeName) {
    if (valueSchema.getType() == Schema.Type.STRING) {
      // For string schema, only "value" or "_value" are valid field names
      for (String fieldName: fieldNames) {
        if (!fieldName.equals("value") && !fieldName.equals("_value")) {
          return CountByValueResponse.newBuilder()
              .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST)
              .setErrorMessage(
                  "For string-valued stores, only 'value' or '_value' field names are supported. Got: " + fieldName)
              .build();
        }
      }
    } else if (valueSchema.getType() == Schema.Type.RECORD) {
      // For record types, validate fields exist
      for (String fieldName: fieldNames) {
        Schema.Field field = valueSchema.getField(fieldName);
        if (field == null) {
          return CountByValueResponse.newBuilder()
              .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST)
              .setErrorMessage("Field '" + fieldName + "' not found in schema for store: " + storeName)
              .build();
        }
      }
    } else {
      // Other types not supported for countByValue
      return CountByValueResponse.newBuilder()
          .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST)
          .setErrorMessage("CountByValue only supports STRING and RECORD value types. Got: " + valueSchema.getType())
          .build();
    }
    return null; // Validation passed
  }

  /**
   * Process a single value (decompress, deserialize, and count fields).
   * 
   * @param valueBytes Raw value bytes from storage
   * @param fieldNames List of field names to extract and count
   * @param valueSchema The value schema
   * @param valueSchemaEntry Schema entry with ID information
   * @param compressionStrategy Compression strategy used
   * @param compressor Venice compressor instance
   * @param deserializerCache Deserializer cache
   * @param fieldCounts Map to update with field counts
   */
  public static void processValue(
      byte[] valueBytes,
      List<String> fieldNames,
      Schema valueSchema,
      SchemaEntry valueSchemaEntry,
      CompressionStrategy compressionStrategy,
      VeniceCompressor compressor,
      AvroStoreDeserializerCache<Object> deserializerCache,
      Map<String, Map<String, Integer>> fieldCounts) {

    // Decompress if needed
    ByteBuffer valueBuffer = ByteBuffer.wrap(valueBytes);
    ByteBuffer decompressedBuffer;
    try {
      if (compressionStrategy != CompressionStrategy.NO_OP) {
        decompressedBuffer = compressor.decompress(valueBuffer);
      } else {
        decompressedBuffer = valueBuffer;
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to decompress value: {}", e.getMessage());
      return;
    }

    // Deserialize the value - handle both string values and GenericRecord values
    try {
      // Check if the value schema is a simple string type
      if (valueSchema.getType() == Schema.Type.STRING) {
        processStringValue(valueSchemaEntry, decompressedBuffer, deserializerCache, fieldNames, fieldCounts);
      } else {
        processRecordValue(valueSchemaEntry, decompressedBuffer, deserializerCache, fieldNames, fieldCounts);
      }
    } catch (Exception deserializeException) {
      LOGGER.warn("Failed to deserialize value: {}", deserializeException.getMessage(), deserializeException);
    }
  }

  /**
   * Process a string-typed value.
   */
  private static void processStringValue(
      SchemaEntry valueSchemaEntry,
      ByteBuffer decompressedBuffer,
      AvroStoreDeserializerCache<Object> deserializerCache,
      List<String> fieldNames,
      Map<String, Map<String, Integer>> fieldCounts) {

    // For string values, we need to extract the schema ID from the data first
    // Venice stores data with a schema ID prefix
    int readerSchemaId = valueSchemaEntry.getId();

    // Extract writer schema ID from the data (first few bytes)
    int writerSchemaId;
    if (decompressedBuffer.remaining() >= 4) {
      // Schema ID is stored as the first 4 bytes in big-endian format
      writerSchemaId = decompressedBuffer.getInt();
      // Don't reset - we want to advance past the schema ID for deserialization
    } else {
      // Fallback to reader schema ID if data is too short
      writerSchemaId = readerSchemaId;
    }

    RecordDeserializer<Object> deserializer = deserializerCache.getDeserializer(writerSchemaId, readerSchemaId);
    Object deserializedValue = deserializer.deserialize(decompressedBuffer);

    // Use the shared utility to add value to field counts
    addValueToFieldCounts(fieldCounts, deserializedValue, fieldNames);
  }

  /**
   * Process a record-typed value.
   */
  private static void processRecordValue(
      SchemaEntry valueSchemaEntry,
      ByteBuffer decompressedBuffer,
      AvroStoreDeserializerCache<Object> deserializerCache,
      List<String> fieldNames,
      Map<String, Map<String, Integer>> fieldCounts) {

    // Complex GenericRecord value - deserialize and extract fields
    RecordDeserializer<Object> deserializer =
        deserializerCache.getDeserializer(valueSchemaEntry.getId(), valueSchemaEntry.getId());
    Object deserializedValue = deserializer.deserialize(decompressedBuffer);

    if (!(deserializedValue instanceof GenericRecord)) {
      LOGGER.warn("Expected GenericRecord but got: {}", deserializedValue.getClass().getName());
      return;
    }

    // Use the shared utility to add value to field counts
    addValueToFieldCounts(fieldCounts, deserializedValue, fieldNames);
  }

  /**
   * Build CountByValueResponse from field counts.
   * 
   * @param fieldNames List of field names
   * @param fieldCounts Map of field counts
   * @return CountByValueResponse
   */
  public static CountByValueResponse buildResponse(
      List<String> fieldNames,
      Map<String, Map<String, Integer>> fieldCounts) {
    // Build response with all counts (no topK filtering - client will do that)
    CountByValueResponse.Builder responseBuilder = CountByValueResponse.newBuilder();
    for (String fieldName: fieldNames) {
      Map<String, Integer> counts = fieldCounts.get(fieldName);
      if (counts != null && !counts.isEmpty()) {
        responseBuilder.putFieldToValueCounts(fieldName, ValueCount.newBuilder().putAllValueToCounts(counts).build());
      } else {
        // Add empty result for this field
        responseBuilder.putFieldToValueCounts(fieldName, ValueCount.newBuilder().build());
      }
    }

    return responseBuilder.setErrorCode(VeniceReadResponseStatus.OK).build();
  }

  /**
   * Merge multiple partition CountByValue responses into a single result.
   * This method aggregates field counts from multiple partitions into global counts.
   * 
   * @param responses List of CountByValueResponse from different partitions
   * @param fieldNames List of field names to merge
   * @return Merged field counts map where key is field name and value is aggregated counts
   */
  public static Map<String, Map<String, Integer>> mergePartitionResponses(
      List<CountByValueResponse> responses,
      List<String> fieldNames) {

    Map<String, Map<String, Integer>> globalFieldCounts = initializeFieldCounts(fieldNames);

    for (CountByValueResponse response: responses) {
      for (Map.Entry<String, ValueCount> entry: response.getFieldToValueCountsMap().entrySet()) {
        String fieldName = entry.getKey();
        Map<String, Integer> partitionFieldCounts = entry.getValue().getValueToCountsMap();

        Map<String, Integer> globalFieldCount = globalFieldCounts.get(fieldName);
        if (globalFieldCount != null) {
          // Merge partition counts into global counts
          for (Map.Entry<String, Integer> countEntry: partitionFieldCounts.entrySet()) {
            globalFieldCount.merge(countEntry.getKey(), countEntry.getValue(), Integer::sum);
          }
        }
      }
    }

    return globalFieldCounts;
  }

  /**
   * Initialize field counts map.
   * 
   * @param fieldNames List of field names
   * @return Initialized field counts map
   */
  public static Map<String, Map<String, Integer>> initializeFieldCounts(List<String> fieldNames) {
    Map<String, Map<String, Integer>> fieldCounts = new HashMap<>();
    for (String fieldName: fieldNames) {
      fieldCounts.put(fieldName, new HashMap<>());
    }
    return fieldCounts;
  }
}
