package com.linkedin.venice.listener.grpc.handlers;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.client.store.FacetCountingUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.protocols.CountByValueRequest;
import com.linkedin.venice.protocols.CountByValueResponse;
import com.linkedin.venice.protocols.ValueCount;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.AvroStoreDeserializerCache;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.StoreVersionStateUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
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

      // Validate that all fields exist in the schema using shared utility
      try {
        FacetCountingUtils.validateFieldNames(fieldNames.toArray(new String[0]), valueSchema);
      } catch (IllegalArgumentException e) {
        return CountByValueResponse.newBuilder()
            .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST)
            .setErrorMessage(e.getMessage())
            .build();
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
      // Get the partition ID from the request
      int partitionId = request.getPartition();

      // Check if this server has the requested partition
      Set<Integer> availablePartitions = storageEngine.getPersistedPartitionIds();
      if (!availablePartitions.contains(partitionId)) {
        LOGGER.debug(
            "Partition {} not available on this server for store: {}, returning empty results",
            partitionId,
            storeName);
        // Return OK with empty results when partition is not available
        return buildCountByValueResponse(fieldNames, new HashMap<>());
      }

      // Collect deserialized values for shared processing
      List<Object> deserializedValues = new java.util.ArrayList<>();

      // Process all keys sent by client (they all belong to the same partition)
      for (int i = 0; i < request.getKeysCount(); i++) {
        byte[] keyBytes = request.getKeys(i).toByteArray();

        try {
          // Get value from the specified partition
          byte[] valueBytes = storageEngine.get(partitionId, keyBytes);
          if (valueBytes != null) {
            Object deserializedValue = deserializeValue(
                valueBytes,
                valueSchema,
                valueSchemaEntry,
                compressionStrategy,
                compressor,
                deserializerCache);
            if (deserializedValue != null) {
              deserializedValues.add(deserializedValue);
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Error getting key {} from partition {}: {}", i, partitionId, e.getMessage());
        }
      }

      // Use FacetCountingUtils.getValueToCount for each field
      Map<String, Map<Object, Integer>> fieldToValueCounts = new HashMap<>();

      for (String fieldName: fieldNames) {
        // For GenericRecord values, use them directly
        // For String/Utf8 values (string-valued stores), create simple wrapper
        List<GenericRecord> records = new ArrayList<>();
        for (Object value: deserializedValues) {
          if (value instanceof GenericRecord) {
            records.add((GenericRecord) value);
          } else if (value instanceof String || value instanceof Utf8) {
            // For string-valued stores where fieldName is "value" or "_value"
            records.add(new SimpleStringRecord(value));
          }
        }

        // Use FacetCountingUtils.getValueToCount (no TopK limit on server side)
        Map<Object, Integer> valueCounts = FacetCountingUtils.getValueToCount(records, fieldName, Integer.MAX_VALUE);
        fieldToValueCounts.put(fieldName, valueCounts);
      }

      return buildCountByValueResponse(fieldNames, fieldToValueCounts);

    } catch (Exception e) {
      LOGGER.error("Error processing keys for store: {}", storeName, e);
      return CountByValueResponse.newBuilder()
          .setErrorCode(VeniceReadResponseStatus.INTERNAL_ERROR)
          .setErrorMessage("Error processing keys: " + e.getMessage())
          .build();
    }
  }

  // Server-side specific methods

  /**
   * Deserialize a single value from bytes.
   */
  private Object deserializeValue(
      byte[] valueBytes,
      Schema valueSchema,
      SchemaEntry valueSchemaEntry,
      CompressionStrategy compressionStrategy,
      VeniceCompressor compressor,
      AvroStoreDeserializerCache<Object> deserializerCache) {

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
      return null;
    }

    // Deserialize the value - handle both string values and GenericRecord values
    try {
      // Check if the value schema is a simple string type
      if (valueSchema.getType() == Schema.Type.STRING) {
        return deserializeStringValue(valueSchemaEntry, decompressedBuffer, deserializerCache);
      } else {
        return deserializeRecordValue(valueSchemaEntry, decompressedBuffer, deserializerCache);
      }
    } catch (Exception deserializeException) {
      LOGGER.warn("Failed to deserialize value: {}", deserializeException.getMessage(), deserializeException);
      return null;
    }
  }

  /**
   * Deserialize a string-typed value.
   */
  private Object deserializeStringValue(
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
  private Object deserializeRecordValue(
      SchemaEntry valueSchemaEntry,
      ByteBuffer decompressedBuffer,
      AvroStoreDeserializerCache<Object> deserializerCache) {

    RecordDeserializer<Object> deserializer =
        deserializerCache.getDeserializer(valueSchemaEntry.getId(), valueSchemaEntry.getId());
    Object deserializedValue = deserializer.deserialize(decompressedBuffer);

    if (!(deserializedValue instanceof GenericRecord) && !(deserializedValue instanceof String)
        && !(deserializedValue instanceof Utf8)) {
      LOGGER.warn("Expected GenericRecord or String but got: {}", deserializedValue.getClass().getName());
      return null;
    }

    return deserializedValue;
  }

  /**
   * Build CountByValueResponse from field counts.
   */
  private CountByValueResponse buildCountByValueResponse(
      List<String> fieldNames,
      Map<String, Map<Object, Integer>> fieldToValueCounts) {
    CountByValueResponse.Builder responseBuilder = CountByValueResponse.newBuilder();
    for (String fieldName: fieldNames) {
      Map<Object, Integer> counts = fieldToValueCounts.get(fieldName);
      if (counts != null && !counts.isEmpty()) {
        // Convert Object keys to String keys for the protocol buffer
        Map<String, Integer> stringCounts = new LinkedHashMap<>();
        for (Map.Entry<Object, Integer> entry: counts.entrySet()) {
          String key = (entry.getKey() == null) ? "null" : entry.getKey().toString();
          stringCounts.put(key, entry.getValue());
        }
        responseBuilder
            .putFieldToValueCounts(fieldName, ValueCount.newBuilder().putAllValueToCounts(stringCounts).build());
      } else {
        responseBuilder.putFieldToValueCounts(fieldName, ValueCount.newBuilder().build());
      }
    }
    return responseBuilder.setErrorCode(VeniceReadResponseStatus.OK).build();
  }

  /**
   * Minimal wrapper for string values in string-valued stores.
   */
  private static class SimpleStringRecord implements GenericRecord {
    private final Object value;

    SimpleStringRecord(Object value) {
      this.value = value instanceof Utf8 ? value.toString() : value;
    }

    @Override
    public Object get(String key) {
      if ("value".equals(key) || "_value".equals(key)) {
        return value;
      }
      return null;
    }

    @Override
    public Object get(int i) {
      return null;
    }

    @Override
    public void put(String key, Object v) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void put(int i, Object v) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Schema getSchema() {
      return Schema.create(Schema.Type.STRING);
    }
  }
}
