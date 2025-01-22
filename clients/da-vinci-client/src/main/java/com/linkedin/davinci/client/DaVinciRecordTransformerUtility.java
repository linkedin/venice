package com.linkedin.davinci.client;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStorageIterator;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import org.apache.avro.Schema;


/**
 * Utility class for {@link DaVinciRecordTransformer}.
 *
 * @param <K> the type of the input key
 * @param <O> the type of the output value
 */
public class DaVinciRecordTransformerUtility<K, O> {
  private final DaVinciRecordTransformer recordTransformer;
  private final AvroGenericDeserializer<K> keyDeserializer;
  private final AvroGenericDeserializer<O> outputValueDeserializer;
  private final AvroSerializer<O> outputValueSerializer;

  public DaVinciRecordTransformerUtility(DaVinciRecordTransformer recordTransformer) {
    this.recordTransformer = recordTransformer;

    Schema keySchema = recordTransformer.getKeySchema();
    Schema outputValueSchema = recordTransformer.getOutputValueSchema();
    this.keyDeserializer = new AvroGenericDeserializer<>(keySchema, keySchema);
    this.outputValueDeserializer = new AvroGenericDeserializer<>(outputValueSchema, outputValueSchema);
    this.outputValueSerializer = new AvroSerializer<>(outputValueSchema);
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
  public boolean hasTransformerLogicChanged(
      AbstractStorageEngine storageEngine,
      int partitionId,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer,
      int currentClassHash) {
    Optional<OffsetRecord> optionalOffsetRecord = storageEngine.getPartitionOffset(partitionId);
    OffsetRecord offsetRecord = optionalOffsetRecord.orElseGet(() -> new OffsetRecord(partitionStateSerializer));
    Integer persistedClassHash = offsetRecord.getRecordTransformerClassHash();

    if (Objects.equals(persistedClassHash, currentClassHash)) {
      return false;
    }

    offsetRecord.setRecordTransformerClassHash(currentClassHash);
    storageEngine.putPartitionOffset(partitionId, offsetRecord);
    return true;
  }

  /**
   * Bootstraps the client after it comes online.
   */
  public final void onRecovery(
      AbstractStorageEngine storageEngine,
      int partitionId,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer,
      Lazy<VeniceCompressor> compressor) {
    int classHash = recordTransformer.getClassHash();
    boolean transformerLogicChanged =
        hasTransformerLogicChanged(storageEngine, partitionId, partitionStateSerializer, classHash);

    if (!recordTransformer.getStoreRecordsInDaVinci() || transformerLogicChanged) {
      // Bootstrap from VT
      storageEngine.clearPartitionOffset(partitionId);
    } else {
      // Bootstrap from local storage
      AbstractStorageIterator iterator = storageEngine.getIterator(partitionId);
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        byte[] keyBytes = iterator.key();
        byte[] valueBytes = iterator.value();
        Lazy<K> lazyKey = Lazy.of(() -> keyDeserializer.deserialize(keyBytes));
        Lazy<O> lazyValue = Lazy.of(() -> {
          ByteBuffer valueByteBuffer = ByteBuffer.wrap(valueBytes);
          // Skip schema id
          valueByteBuffer.position(Integer.BYTES);
          ByteBuffer decompressedValueBytes;
          try {
            decompressedValueBytes = compressor.get().decompress(valueByteBuffer);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return outputValueDeserializer.deserialize(decompressedValueBytes);
        });

        recordTransformer.processPut(lazyKey, lazyValue);
      }
    }
  }
}
