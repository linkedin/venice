package com.linkedin.venice.utils;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.protocols.BucketCount;
import com.linkedin.venice.protocols.BucketPredicate;
import com.linkedin.venice.protocols.CountByBucketResponse;
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
 * Utility class for CountByBucket aggregation operations.
 * This class provides shared implementation for processing values and counting bucket occurrences
 * that can be used by both server-side and client-side components.
 */
public class CountByBucketUtils {
  private static final Logger LOGGER = LogManager.getLogger(CountByBucketUtils.class);

  private CountByBucketUtils() {
    // Utility class
  }

  /**
   * Initialize bucket counts map.
   * 
   * @param fieldNames List of field names
   * @param bucketNames List of bucket names
   * @return Initialized bucket counts map
   */
  public static Map<String, Map<String, Integer>> initializeBucketCounts(
      List<String> fieldNames,
      List<String> bucketNames) {
    Map<String, Map<String, Integer>> bucketCounts = new HashMap<>();
    for (String fieldName: fieldNames) {
      Map<String, Integer> fieldBuckets = new HashMap<>();
      for (String bucketName: bucketNames) {
        fieldBuckets.put(bucketName, 0);
      }
      bucketCounts.put(fieldName, fieldBuckets);
    }
    return bucketCounts;
  }

  /**
   * Process a single value for bucket counting.
   * 
   * @param valueBytes Raw value bytes from storage
   * @param fieldNames List of field names to extract and count
   * @param bucketPredicates Map of bucket names to predicates
   * @param valueSchema The value schema
   * @param valueSchemaEntry Schema entry with ID information
   * @param compressionStrategy Compression strategy used
   * @param compressor Venice compressor instance
   * @param deserializerCache Deserializer cache
   * @param bucketCounts Map to update with bucket counts
   */
  public static void processValueForBuckets(
      byte[] valueBytes,
      List<String> fieldNames,
      Map<String, BucketPredicate> bucketPredicates,
      Schema valueSchema,
      SchemaEntry valueSchemaEntry,
      CompressionStrategy compressionStrategy,
      VeniceCompressor compressor,
      AvroStoreDeserializerCache<Object> deserializerCache,
      Map<String, Map<String, Integer>> bucketCounts) {

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

    // Deserialize the value
    try {
      Object deserializedValue;
      if (valueSchema.getType() == Schema.Type.STRING) {
        deserializedValue = deserializeStringValue(valueSchemaEntry, decompressedBuffer, deserializerCache);
      } else {
        deserializedValue = deserializeRecordValue(valueSchemaEntry, decompressedBuffer, deserializerCache);
      }

      // Process bucket counting
      evaluateBucketsForValue(deserializedValue, fieldNames, bucketPredicates, bucketCounts);

    } catch (Exception e) {
      LOGGER.warn("Failed to deserialize value: {}", e.getMessage());
    }
  }

  /**
   * Deserialize a string-typed value.
   */
  private static Object deserializeStringValue(
      SchemaEntry valueSchemaEntry,
      ByteBuffer decompressedBuffer,
      AvroStoreDeserializerCache<Object> deserializerCache) {

    int readerSchemaId = valueSchemaEntry.getId();
    int writerSchemaId;
    if (decompressedBuffer.remaining() >= 4) {
      writerSchemaId = decompressedBuffer.getInt();
    } else {
      writerSchemaId = readerSchemaId;
    }

    RecordDeserializer<Object> deserializer = deserializerCache.getDeserializer(writerSchemaId, readerSchemaId);
    return deserializer.deserialize(decompressedBuffer);
  }

  /**
   * Deserialize a record-typed value.
   */
  private static Object deserializeRecordValue(
      SchemaEntry valueSchemaEntry,
      ByteBuffer decompressedBuffer,
      AvroStoreDeserializerCache<Object> deserializerCache) {

    RecordDeserializer<Object> deserializer =
        deserializerCache.getDeserializer(valueSchemaEntry.getId(), valueSchemaEntry.getId());
    return deserializer.deserialize(decompressedBuffer);
  }

  /**
   * Evaluate buckets for a value and update counts.
   */
  private static void evaluateBucketsForValue(
      Object value,
      List<String> fieldNames,
      Map<String, BucketPredicate> bucketPredicates,
      Map<String, Map<String, Integer>> bucketCounts) {

    if (value == null) {
      return;
    }

    for (String fieldName: fieldNames) {
      Object fieldValue = extractFieldValue(value, fieldName);
      if (fieldValue == null) {
        continue;
      }

      // Evaluate each bucket predicate
      for (Map.Entry<String, BucketPredicate> entry: bucketPredicates.entrySet()) {
        String bucketName = entry.getKey();
        BucketPredicate predicate = entry.getValue();

        if (PredicateEvaluator.evaluate(predicate, fieldValue)) {
          bucketCounts.get(fieldName).merge(bucketName, 1, Integer::sum);
        }
      }
    }
  }

  /**
   * Extract field value from a value object.
   * Reuses logic from CountByValueUtils.
   */
  private static Object extractFieldValue(Object value, String fieldName) {
    if (value == null) {
      return null;
    }

    // For string-valued stores
    if ((value instanceof String || value instanceof Utf8)
        && ("value".equals(fieldName) || "_value".equals(fieldName))) {
      return value instanceof Utf8 ? ((Utf8) value).toString() : value;
    }

    // For record-valued stores
    if (value instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) value;
      if (record.getSchema().getField(fieldName) != null) {
        Object fieldValue = record.get(fieldName);
        if (fieldValue instanceof Utf8) {
          return fieldValue.toString();
        }
        return fieldValue;
      }
    }

    return null;
  }

  /**
   * Validate that field names are valid for the given schema.
   * 
   * @param fieldNames List of field names to validate
   * @param valueSchema The schema to validate against
   * @param storeName Store name for error messages
   * @return Error response if validation fails, null if validation passes
   */
  public static CountByBucketResponse validateFields(List<String> fieldNames, Schema valueSchema, String storeName) {
    if (valueSchema.getType() == Schema.Type.STRING) {
      // For string schema, only "value" or "_value" are valid field names
      for (String fieldName: fieldNames) {
        if (!fieldName.equals("value") && !fieldName.equals("_value")) {
          return CountByBucketResponse.newBuilder()
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
          return CountByBucketResponse.newBuilder()
              .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST)
              .setErrorMessage("Field '" + fieldName + "' not found in schema for store: " + storeName)
              .build();
        }
      }
    } else {
      // Other types not supported for countByBucket
      return CountByBucketResponse.newBuilder()
          .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST)
          .setErrorMessage("CountByBucket only supports STRING and RECORD value types. Got: " + valueSchema.getType())
          .build();
    }
    return null; // Validation passed
  }

  /**
   * Build CountByBucketResponse from bucket counts.
   * 
   * @param fieldNames List of field names
   * @param bucketCounts Map of bucket counts
   * @return CountByBucketResponse
   */
  public static CountByBucketResponse buildResponse(
      List<String> fieldNames,
      Map<String, Map<String, Integer>> bucketCounts) {
    CountByBucketResponse.Builder responseBuilder = CountByBucketResponse.newBuilder();

    for (String fieldName: fieldNames) {
      Map<String, Integer> counts = bucketCounts.get(fieldName);
      if (counts != null) {
        responseBuilder
            .putFieldToBucketCounts(fieldName, BucketCount.newBuilder().putAllBucketToCounts(counts).build());
      } else {
        // Add empty result for this field
        responseBuilder.putFieldToBucketCounts(fieldName, BucketCount.newBuilder().build());
      }
    }

    return responseBuilder.setErrorCode(VeniceReadResponseStatus.OK).build();
  }

  /**
   * Merge multiple partition CountByBucket responses into a single result.
   * 
   * @param responses List of CountByBucketResponse from different partitions
   * @param fieldNames List of field names to merge
   * @param bucketNames List of bucket names
   * @return Merged bucket counts map
   */
  public static Map<String, Map<String, Integer>> mergePartitionResponses(
      List<CountByBucketResponse> responses,
      List<String> fieldNames,
      List<String> bucketNames) {

    Map<String, Map<String, Integer>> globalBucketCounts = initializeBucketCounts(fieldNames, bucketNames);

    for (CountByBucketResponse response: responses) {
      for (Map.Entry<String, BucketCount> entry: response.getFieldToBucketCountsMap().entrySet()) {
        String fieldName = entry.getKey();
        Map<String, Integer> partitionBucketCounts = entry.getValue().getBucketToCountsMap();

        Map<String, Integer> globalFieldBucketCount = globalBucketCounts.get(fieldName);
        if (globalFieldBucketCount != null) {
          // Merge partition counts into global counts
          for (Map.Entry<String, Integer> countEntry: partitionBucketCounts.entrySet()) {
            globalFieldBucketCount.merge(countEntry.getKey(), countEntry.getValue(), Integer::sum);
          }
        }
      }
    }

    return globalBucketCounts;
  }
}
