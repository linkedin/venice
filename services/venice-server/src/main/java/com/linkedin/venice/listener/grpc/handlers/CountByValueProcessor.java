package com.linkedin.venice.listener.grpc.handlers;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.protocols.CountByValueRequest;
import com.linkedin.venice.protocols.CountByValueResponse;
import com.linkedin.venice.protocols.ValueCount;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.AvroStoreDeserializerCache;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.CountByValueUtils;
import com.linkedin.venice.utils.StoreVersionStateUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Processor for handling CountByValue aggregation requests.
 * This class encapsulates the logic for processing countByValue operations on Venice stores.
 */
public class CountByValueProcessor {
  private static final Logger LOGGER = LogManager.getLogger(CountByValueProcessor.class);

  private final StorageEngineRepository storageEngineRepository;
  private final ReadOnlySchemaRepository schemaRepository;
  private final ReadOnlyStoreRepository storeRepository;
  private final StorageEngineBackedCompressorFactory compressorFactory;
  private final Map<String, AvroStoreDeserializerCache<Object>> storeDeserializerCacheMap;
  private final VenicePartitioner partitioner;

  public CountByValueProcessor(
      StorageEngineRepository storageEngineRepository,
      ReadOnlySchemaRepository schemaRepository,
      ReadOnlyStoreRepository storeRepository,
      StorageEngineBackedCompressorFactory compressorFactory) {
    this.storageEngineRepository = storageEngineRepository;
    this.schemaRepository = schemaRepository;
    this.storeRepository = storeRepository;
    this.compressorFactory = compressorFactory;
    this.storeDeserializerCacheMap = new VeniceConcurrentHashMap<>();
    this.partitioner = new DefaultVenicePartitioner();
  }

  /**
   * Process countByValue aggregation request with real data processing.
   */
  public CountByValueResponse processCountByValue(CountByValueRequest request) {
    // If dependencies are not available, return graceful error
    if (storeRepository == null || schemaRepository == null || storageEngineRepository == null
        || compressorFactory == null) {
      LOGGER.warn("CountByValue dependencies not available");
      return CountByValueResponse.newBuilder()
          .setErrorCode(VeniceReadResponseStatus.INTERNAL_ERROR)
          .setErrorMessage("CountByValue dependencies not available")
          .build();
    }

    try {
      String resourceName = request.getResourceName();
      List<String> fieldNames = request.getFieldNamesList();

      // Validate inputs
      if (fieldNames == null || fieldNames.isEmpty()) {
        return CountByValueResponse.newBuilder()
            .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST)
            .setErrorMessage("Field names cannot be null or empty")
            .build();
      }

      // Parse store name and version from resource name using Venice Version util
      String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
      if (storeName.isEmpty()) {
        return CountByValueResponse.newBuilder()
            .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST)
            .setErrorMessage("Invalid resource name format. Expected: storeName_v{version}")
            .build();
      }

      int version;
      try {
        version = Version.parseVersionFromKafkaTopicName(resourceName);
      } catch (Exception e) {
        return CountByValueResponse.newBuilder()
            .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST)
            .setErrorMessage("Invalid version number in resource name: " + e.getMessage())
            .build();
      }

      // Get store and validate it exists
      Store store = storeRepository.getStoreOrThrow(storeName);
      if (!store.containsVersion(version)) {
        return CountByValueResponse.newBuilder()
            .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST)
            .setErrorMessage("Store version " + version + " does not exist for store " + storeName)
            .build();
      }

      // Get storage engine for this store version
      String topicName = Version.composeKafkaTopic(storeName, version);
      StorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(topicName);
      if (storageEngine == null) {
        return CountByValueResponse.newBuilder()
            .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST)
            .setErrorMessage("Storage engine not found for topic: " + topicName)
            .build();
      }

      // Get the latest value schema for this store
      SchemaEntry valueSchemaEntry = schemaRepository.getSupersetOrLatestValueSchema(storeName);
      if (valueSchemaEntry == null) {
        return CountByValueResponse.newBuilder()
            .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST)
            .setErrorMessage("No value schema found for store: " + storeName)
            .build();
      }

      Schema valueSchema = valueSchemaEntry.getSchema();

      // Validate that all fields exist in the schema
      CountByValueResponse validationResponse = validateFields(fieldNames, valueSchema, storeName);
      if (validationResponse != null) {
        return validationResponse;
      }

      // Get store version state for compression info
      StoreVersionState storeVersionState = storageEngine.getStoreVersionState();
      if (storeVersionState == null) {
        return CountByValueResponse.newBuilder()
            .setErrorCode(VeniceReadResponseStatus.INTERNAL_ERROR)
            .setErrorMessage("Store version state not available")
            .build();
      }

      CompressionStrategy compressionStrategy = StoreVersionStateUtils.getCompressionStrategy(storeVersionState);
      VeniceCompressor compressor = compressorFactory.getCompressor(compressionStrategy);

      // Get deserializer cache for this store - use Object type to handle both GenericRecord and String values
      AvroStoreDeserializerCache<Object> deserializerCache = storeDeserializerCacheMap
          .computeIfAbsent(storeName, k -> new AvroStoreDeserializerCache<>(schemaRepository, storeName, true));

      return processKeys(
          request,
          fieldNames,
          storageEngine,
          valueSchema,
          valueSchemaEntry,
          compressionStrategy,
          compressor,
          deserializerCache,
          storeName);

    } catch (VeniceNoStoreException e) {
      return CountByValueResponse.newBuilder()
          .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST)
          .setErrorMessage("Store not found: " + e.getStoreName())
          .build();
    } catch (Exception e) {
      LOGGER.error("Error in processCountByValue for store: " + request.getResourceName(), e);
      return CountByValueResponse.newBuilder()
          .setErrorCode(VeniceReadResponseStatus.INTERNAL_ERROR)
          .setErrorMessage("Internal error: " + e.getMessage())
          .build();
    }
  }

  private CountByValueResponse validateFields(List<String> fieldNames, Schema valueSchema, String storeName) {
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

  private CountByValueResponse processKeys(
      CountByValueRequest request,
      List<String> fieldNames,
      StorageEngine storageEngine,
      Schema valueSchema,
      SchemaEntry valueSchemaEntry,
      CompressionStrategy compressionStrategy,
      VeniceCompressor compressor,
      AvroStoreDeserializerCache<Object> deserializerCache,
      String storeName) {

    try {
      // Initialize field counts
      Map<String, Map<String, Integer>> fieldCounts = new HashMap<>();
      for (String fieldName: fieldNames) {
        fieldCounts.put(fieldName, new HashMap<>());
      }

      // Get all partition IDs available on this server
      Set<Integer> availablePartitions = storageEngine.getPersistedPartitionIds();

      // Get total partition count for this store version
      int versionNumber = Version.parseVersionFromKafkaTopicName(request.getResourceName());
      Store store = storeRepository.getStoreOrThrow(storeName);
      Version version = store.getVersion(versionNumber);
      int totalPartitions = version.getPartitionCount();

      // Process all keys sent by client
      for (int i = 0; i < request.getKeysCount(); i++) {
        byte[] keyBytes = request.getKeys(i).toByteArray();

        try {
          // Calculate the correct partition for this key using the same algorithm as client
          int targetPartition = partitioner.getPartitionId(keyBytes, totalPartitions);

          // Check if this server has the target partition
          if (availablePartitions.contains(targetPartition)) {
            try {
              byte[] valueBytes = storageEngine.get(targetPartition, keyBytes);
              if (valueBytes != null) {
                processValue(
                    valueBytes,
                    fieldNames,
                    valueSchema,
                    valueSchemaEntry,
                    compressionStrategy,
                    compressor,
                    deserializerCache,
                    fieldCounts);
              }
            } catch (Exception e) {
              LOGGER.warn("Error getting key {} from partition {}: {}", i, targetPartition, e.getMessage());
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Error processing key {}: {}", i, e.getMessage());
        }
      }

      return buildResponse(fieldNames, fieldCounts);

    } catch (Exception e) {
      LOGGER.error("Error processing keys for store: {}", storeName, e);
      return CountByValueResponse.newBuilder()
          .setErrorCode(VeniceReadResponseStatus.INTERNAL_ERROR)
          .setErrorMessage("Error processing keys: " + e.getMessage())
          .build();
    }
  }

  private void processValue(
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

  private void processStringValue(
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
    CountByValueUtils.addValueToFieldCounts(fieldCounts, deserializedValue, fieldNames);
  }

  private void processRecordValue(
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
    CountByValueUtils.addValueToFieldCounts(fieldCounts, deserializedValue, fieldNames);
  }

  private CountByValueResponse buildResponse(List<String> fieldNames, Map<String, Map<String, Integer>> fieldCounts) {
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
}
