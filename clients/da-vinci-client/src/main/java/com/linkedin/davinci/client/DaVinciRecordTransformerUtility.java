package com.linkedin.davinci.client;

import com.linkedin.davinci.store.AbstractStorageIterator;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StoragePartitionAdjustmentTrigger;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.VeniceSerializationException;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Utility class for {@link DaVinciRecordTransformer}.
 *
 * @param <K> the type of the input key
 * @param <O> the type of the output value
 */
public class DaVinciRecordTransformerUtility<K, O> {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciRecordTransformerUtility.class);
  private final DaVinciRecordTransformer recordTransformer;
  private final DaVinciRecordTransformerConfig recordTransformerConfig;
  private final RecordDeserializer<K> keyDeserializer;
  private final RecordSerializer<O> outputValueSerializer;

  public DaVinciRecordTransformerUtility(
      DaVinciRecordTransformer recordTransformer,
      DaVinciRecordTransformerConfig recordTransformerConfig) {
    this.recordTransformer = recordTransformer;
    this.recordTransformerConfig = recordTransformerConfig;

    Schema keySchema = recordTransformer.getKeySchema();
    if (recordTransformerConfig.useSpecificRecordKeyDeserializer()) {
      this.keyDeserializer = FastSerializerDeserializerFactory
          .getFastAvroSpecificDeserializer(keySchema, recordTransformerConfig.getKeyClass());
    } else {
      this.keyDeserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(keySchema, keySchema);
    }

    this.outputValueSerializer =
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(recordTransformer.getOutputValueSchema());
  }

  /**
   * Serializes and compresses the value and prepends the schema ID to the resulting ByteBuffer.
   *
   * @param value to be serialized and compressed
   * @param schemaId to prepend to the ByteBuffer
   * @return a ByteBuffer containing the schema ID followed by the serialized and compressed value
   */
  public final ByteBuffer prependSchemaIdToHeader(O value, int schemaId, VeniceCompressor compressor) {
    byte[] serializedValue = outputValueSerializer.serialize(value);
    byte[] compressedValue;
    try {
      compressedValue = compressor.compress(serializedValue);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ByteBuffer valueBytes = ByteBuffer.wrap(compressedValue);
    ByteBuffer newValueBytes = ByteBuffer.allocate(Integer.BYTES + valueBytes.remaining());
    newValueBytes.putInt(schemaId);
    newValueBytes.put(valueBytes);
    newValueBytes.flip();
    return newValueBytes;
  }

  /**
   * Prepends the given schema ID to the provided ByteBuffer
   *
   * @param valueBytes the original serialized and compressed value
   * @param schemaId to prepend to the ByteBuffer
   * @return a ByteBuffer containing the schema ID followed by the serialized and compressed value
   */
  public final ByteBuffer prependSchemaIdToHeader(ByteBuffer valueBytes, int schemaId) {
    ByteBuffer newBuffer = ByteBuffer.allocate(Integer.BYTES + valueBytes.remaining());
    newBuffer.putInt(schemaId);
    newBuffer.put(valueBytes);
    newBuffer.flip();
    return newBuffer;
  }

  /**
   * @return true if transformer logic has changed since the last time the class was loaded
   */
  public boolean hasTransformerLogicChanged(int currentClassHash, OffsetRecord offsetRecord) {
    if (!recordTransformerConfig.isRecordTransformationEnabled()) {
      LOGGER.info("Record transformation is disabled for DaVinciRecordTransformer. Skipping classHash comparison.");
      return false;
    }

    Integer persistedClassHash = offsetRecord.getRecordTransformerClassHash();

    if (persistedClassHash != null && persistedClassHash == currentClassHash) {
      LOGGER.info("Transformer logic hasn't changed. Class hash: {}", currentClassHash);
      return false;
    }
    LOGGER.info(
        "A change in transformer logic has been detected. Persisted class hash: {}. New class hash: {}",
        persistedClassHash,
        currentClassHash);
    return true;
  }

  /**
   * Bootstraps the client after it comes online.
   */
  public final void onRecovery(
      StorageEngine storageEngine,
      int partitionId,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer,
      Lazy<VeniceCompressor> compressor,
      PubSubContext pubSubContext,
      Map<Integer, Schema> schemaIdToSchemaMap,
      ReadOnlySchemaRepository schemaRepository) {
    int classHash = recordTransformer.getClassHash();
    Optional<OffsetRecord> optionalOffsetRecord = storageEngine.getPartitionOffset(partitionId, pubSubContext);
    OffsetRecord offsetRecord =
        optionalOffsetRecord.orElseGet(() -> new OffsetRecord(partitionStateSerializer, pubSubContext));

    boolean transformerLogicChanged = hasTransformerLogicChanged(classHash, offsetRecord);

    if (recordTransformer.getAlwaysBootstrapFromVersionTopic() || transformerLogicChanged) {
      LOGGER.info("Bootstrapping directly from the VersionTopic for partition: {}", partitionId);

      // Bootstrap from VT
      storageEngine.clearPartitionOffset(partitionId);

      // Offset record is deleted, so create a new one and persist it
      offsetRecord = new OffsetRecord(partitionStateSerializer, pubSubContext);
      offsetRecord.setRecordTransformerClassHash(classHash);
      storageEngine.putPartitionOffset(partitionId, offsetRecord);
    } else if (!recordTransformerConfig.getStoreRecordsInDaVinci()) {
      LOGGER.info("StoreRecordsInDaVinci is set to false. Skipping local database scan.");
    } else {
      // Bootstrap from local storage
      LOGGER.info("Bootstrapping from local storage for partition {}", partitionId);

      // Open DB in read-only mode as a safeguard against updates happening during iteration
      StoragePartitionConfig storagePartitionConfig =
          new StoragePartitionConfig(storageEngine.getStoreVersionName(), partitionId);
      storagePartitionConfig.setReadOnly(true);
      storageEngine.adjustStoragePartition(
          partitionId,
          StoragePartitionAdjustmentTrigger.PREPARE_FOR_READ,
          storagePartitionConfig);

      try (AbstractStorageIterator iterator = storageEngine.getIterator(partitionId)) {
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
          byte[] keyBytes = iterator.key();
          byte[] valueBytes = iterator.value();
          Lazy<K> lazyKey = Lazy.of(() -> keyDeserializer.deserialize(keyBytes));
          Lazy<O> lazyValue = Lazy.of(() -> {
            ByteBuffer valueByteBuffer = ByteBuffer.wrap(valueBytes);

            /*
             * Use writer schema for deserialization, otherwise it will run into deserialization errors if
             * schema evolution occurred.
             */
            int writerSchemaId = valueByteBuffer.getInt();
            Schema valueSchema = schemaIdToSchemaMap.computeIfAbsent(
                writerSchemaId,
                i -> schemaRepository.getValueSchema(recordTransformer.getStoreName(), writerSchemaId).getSchema());

            ByteBuffer decompressedValueBytes;
            try {
              decompressedValueBytes = compressor.get().decompress(valueByteBuffer);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }

            RecordDeserializer<O> outputValueDeserializer;
            if (recordTransformerConfig.useSpecificRecordValueDeserializer()) {
              outputValueDeserializer = FastSerializerDeserializerFactory
                  .getFastAvroSpecificDeserializer(valueSchema, recordTransformerConfig.getOutputValueClass());
            } else {
              if (recordTransformerConfig.isRecordTransformationEnabled()) {
                outputValueDeserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(
                    recordTransformer.getOutputValueSchema(),
                    recordTransformer.getOutputValueSchema());
              } else if (recordTransformer.useUniformInputValueSchema()) {
                outputValueDeserializer = FastSerializerDeserializerFactory
                    .getFastAvroGenericDeserializer(valueSchema, recordTransformer.getInputValueSchema());
              } else {
                outputValueDeserializer =
                    FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(valueSchema, valueSchema);
              }
            }
            return outputValueDeserializer.deserialize(decompressedValueBytes);
          });

          // Most of the record metadata is not available from disk
          DaVinciRecordTransformerRecordMetadata recordTransformerRecordMetadata =
              recordTransformerConfig.isRecordMetadataEnabled()
                  ? new DaVinciRecordTransformerRecordMetadata(
                      /*
                       * We can technically supply the writer schema id, but that would require pulling some logic
                       * out of lazyValue, which could add latency. Revisit if it is actually needed.
                       */
                      0,
                      PubSubSymbolicPosition.EARLIEST,
                      keyBytes.length + valueBytes.length)
                  : null;
          recordTransformer.processPut(lazyKey, lazyValue, partitionId, recordTransformerRecordMetadata);
        }
      } catch (VeniceSerializationException exception) {
        LOGGER.error(
            "VeniceSerializationException encountered when DaVinciRecordTransformer was scanning its local disk for"
                + " records for store: {} and version: {}. This occurs when the wrong schema is used to deserialize"
                + " records. If you are not transforming your records, make sure to set recordTransformationEnabled"
                + " to false in DaVinciRecordTransformerConfig",
            recordTransformer.getStoreName(),
            recordTransformer.getStoreVersion(),
            exception);
        throw exception;
      } finally {
        // Re-open partition with defaults
        storagePartitionConfig = new StoragePartitionConfig(storageEngine.getStoreVersionName(), partitionId);
        storageEngine.adjustStoragePartition(
            partitionId,
            StoragePartitionAdjustmentTrigger.REOPEN_WITH_DEFAULTS,
            storagePartitionConfig);
      }
    }
  }

  @VisibleForTesting
  public RecordDeserializer<K> getKeyDeserializer() {
    return keyDeserializer;
  }
}
