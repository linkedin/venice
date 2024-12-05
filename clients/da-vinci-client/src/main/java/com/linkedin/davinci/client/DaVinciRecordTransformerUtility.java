package com.linkedin.davinci.client;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStorageIterator;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.offsets.OffsetRecord;
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
  private AvroGenericDeserializer<K> keyDeserializer;
  private AvroGenericDeserializer<O> outputValueDeserializer;
  private AvroSerializer<O> outputValueSerializer;

  public DaVinciRecordTransformerUtility(DaVinciRecordTransformer recordTransformer) {
    this.recordTransformer = recordTransformer;
  }

  /**
   * Serializes and compresses the value and prepends the schema ID to the resulting ByteBuffer.
   *
   * @param value to be serialized and compressed
   * @param schemaId to prepend to the ByteBuffer
   * @return a ByteBuffer containing the schema ID followed by the serialized and compressed value
   */
  public final ByteBuffer prependSchemaIdToHeader(O value, int schemaId, VeniceCompressor compressor) {
    byte[] serializedValue = getOutputValueSerializer().serialize(value);
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
  public boolean hasTransformerLogicChanged(AbstractStorageEngine storageEngine, int partitionId, int classHash) {
    try {
      Optional<OffsetRecord> offsetRecord = storageEngine.getPartitionOffset(partitionId);
      Integer offsetRecordClassHash = offsetRecord.map(OffsetRecord::getTransformerClassHash).orElse(null);
      if (!Objects.equals(offsetRecordClassHash, classHash)) {
        offsetRecord.ifPresent(record -> {
          record.setTransformerClassHash(classHash);
          storageEngine.putPartitionOffset(partitionId, record);
        });
        return true;
      } else {
        return false;
      }
    } catch (IllegalArgumentException | StorageInitializationException e) {
      throw new VeniceException("Failed to check if transformation logic has changed", e);
    }
  }

  /**
   * Bootstraps the client after it comes online.
   */
  public final void onRecovery(
      AbstractStorageEngine storageEngine,
      int partitionId,
      Lazy<VeniceCompressor> compressor) {
    int classHash = recordTransformer.getClassHash();
    boolean transformerLogicChanged = hasTransformerLogicChanged(storageEngine, partitionId, classHash);

    if (!recordTransformer.getStoreRecordsInDaVinci() || transformerLogicChanged) {
      // Bootstrap from VT
      storageEngine.clearPartitionOffset(partitionId);
    } else {
      // Bootstrap from local storage
      AbstractStorageIterator iterator = storageEngine.getIterator(partitionId);
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        byte[] keyBytes = iterator.key();
        byte[] valueBytes = iterator.value();
        Lazy<K> lazyKey = Lazy.of(() -> getKeyDeserializer().deserialize(ByteBuffer.wrap(keyBytes)));
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
          return getOutputValueDeserializer().deserialize(decompressedValueBytes);
        });

        recordTransformer.processPut(lazyKey, lazyValue);
      }
    }
  }

  public AvroGenericDeserializer<K> getKeyDeserializer() {
    if (keyDeserializer == null) {
      Schema keySchema = recordTransformer.getKeySchema();
      keyDeserializer = new AvroGenericDeserializer<>(keySchema, keySchema);
    }
    return keyDeserializer;
  }

  public AvroGenericDeserializer<O> getOutputValueDeserializer() {
    if (outputValueDeserializer == null) {
      Schema outputValueSchema = recordTransformer.getOutputValueSchema();
      outputValueDeserializer = new AvroGenericDeserializer<>(outputValueSchema, outputValueSchema);
    }
    return outputValueDeserializer;
  }

  public AvroSerializer<O> getOutputValueSerializer() {
    if (outputValueSerializer == null) {
      Schema outputValueSchema = recordTransformer.getOutputValueSchema();
      outputValueSerializer = new AvroSerializer<>(outputValueSchema);
    }
    return outputValueSerializer;
  }
}
