package com.linkedin.venice.hadoop.input.kafka.chunk;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serializer.AvroSpecificDeserializer;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.specific.SpecificDatumReader;


/**
 * This class accumulates all mapper values and assemble them to provide assembled complete large values or regular
 * message.
 */
public class ChunkAssembler {
  private final Map<ByteBuffer, ByteBuffer> chunksByCompositeKey;
  private final AvroSpecificDeserializer<ChunkedKeySuffix> chunkedKeySuffixDeserializer;
  private final KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer;
  private final ChunkedValueManifestSerializer manifestSerializer;

  public ChunkAssembler() {
    this.chunksByCompositeKey = new HashMap<>();
    SpecificDatumReader<ChunkedKeySuffix> specificDatumReader = new SpecificDatumReader<>(ChunkedKeySuffix.class);
    this.chunkedKeySuffixDeserializer = new AvroSpecificDeserializer<>(specificDatumReader);
    this.keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
    this.manifestSerializer = new ChunkedValueManifestSerializer(true);
  }

  public Optional<ValueBytesAndSchemaId> assembleAndGetValue(
      final byte[] keyBytes,
      final List<KafkaInputMapperValue> values) {
    if (values.isEmpty()) {
      throw new IllegalArgumentException("Expect values to be not empty.");
    }
    try {
      return doAssembleAndGetValue(keyBytes, values);
    } finally {
      reset();
    }
  }

  private Optional<ValueBytesAndSchemaId> doAssembleAndGetValue(
      final byte[] keyBytes,
      final List<KafkaInputMapperValue> values) {
    values.sort(Comparator.comparingLong(value -> value.offset));

    for (int i = values.size() - 1; i >= 0; i--) { // Start from the value with the highest offset
      KafkaInputMapperValue mapperValue = values.get(i);
      if (isRegularMessage(mapperValue)) {
        return mapperValue.valueType == MapperValueType.DELETE
            ? Optional.empty()
            : Optional.of(new ValueBytesAndSchemaId(mapperValue.value, mapperValue.schemaId));

      } else if (mapperValue.schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) { // chunk
                                                                                                                      // manifest
        final ChunkedValueManifest chunkedValueManifest = manifestSerializer.deserialize(
            ByteUtils.extractByteArray(mapperValue.value),
            AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion());
        if (chunksByCompositeKey.isEmpty()) {
          buildChunksByCompositeKeyMapping(keyBytes, values);
        }
        return Optional.of(assembleLargeValue(chunkedValueManifest));

      } else if (mapperValue.schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()) { // chunk value
        // No need to handle it here
      } else {
        throw new VeniceException(
            String.format(
                "Unhandled case with chunked key suffix %s and value schema ID %d",
                deserializeChunkedKeySuffix(mapperValue.chunkedKeySuffix),
                mapperValue.schemaId));
      }
    }
    throw new VeniceException("No regular value nor chunk manifest for key: " + ByteUtils.toHexString(keyBytes));
  }

  private ValueBytesAndSchemaId assembleLargeValue(final ChunkedValueManifest manifest) {
    List<ByteBuffer> allChunkBytes = new ArrayList<>(manifest.keysWithChunkIdSuffix.size());
    int totalByteCount = 0;

    for (int chunkIndex = 0; chunkIndex < manifest.keysWithChunkIdSuffix.size(); chunkIndex++) {
      ByteBuffer compositeKey = manifest.keysWithChunkIdSuffix.get(chunkIndex);
      ByteBuffer chunkBytes = this.chunksByCompositeKey.get(compositeKey);
      if (chunkBytes == null) {
        throw new VeniceException(
            "Cannot assemble a large value. Missing a chunk with the composite key: "
                + ByteUtils.toHexString(ByteUtils.extractByteArray(compositeKey)));
      }
      allChunkBytes.add(chunkBytes);
      totalByteCount += chunkBytes.remaining();
    }
    if (totalByteCount != manifest.size) {
      throw new VeniceException(String.format("Expect %d byte(s) but got %d byte(s)", manifest.size, totalByteCount));
    }
    return new ValueBytesAndSchemaId(concatenateAllChunks(allChunkBytes, totalByteCount), manifest.schemaId);
  }

  private void buildChunksByCompositeKeyMapping(final byte[] keyBytes, final List<KafkaInputMapperValue> values) {
    values.forEach(mapperValue -> {
      if (mapperValue.schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()) {
        ByteBuffer compositeKey =
            createCompositeKey(keyBytes, ByteUtils.extractByteArray(mapperValue.chunkedKeySuffix));
        chunksByCompositeKey.put(compositeKey, mapperValue.value);
      }
    });
  }

  private void reset() {
    chunksByCompositeKey.clear();
  }

  private ChunkedKeySuffix deserializeChunkedKeySuffix(ByteBuffer chunkedKeySuffixBytes) {
    return chunkedKeySuffixDeserializer.deserialize(chunkedKeySuffixBytes);
  }

  private ByteBuffer createCompositeKey(final byte[] keyBytes, final byte[] chunkedKeySuffixBytes) {
    return ByteBuffer.wrap(keyWithChunkingSuffixSerializer.serialize(keyBytes, chunkedKeySuffixBytes));
  }

  private boolean isRegularMessage(KafkaInputMapperValue mapperValue) {
    return mapperValue.schemaId > 0 || mapperValue.valueType == MapperValueType.DELETE;
  }

  private byte[] concatenateAllChunks(final List<ByteBuffer> chunks, final int totalByteCount) {
    byte[] concatenatedChunk = new byte[totalByteCount];
    int currStartingIndexInDst = 0;
    for (ByteBuffer chunk: chunks) {
      final int chunkSizeInBytes = chunk.remaining();
      System.arraycopy(chunk.array(), chunk.position(), concatenatedChunk, currStartingIndexInDst, chunkSizeInBytes);
      currStartingIndexInDst += chunkSizeInBytes;
    }
    return concatenatedChunk;
  }

  public static class ValueBytesAndSchemaId {
    private final byte[] bytes;
    private final int schemaID;

    ValueBytesAndSchemaId(ByteBuffer byteBuffer, int schemaID) {
      this(ByteUtils.extractByteArray(byteBuffer), schemaID);
    }

    ValueBytesAndSchemaId(byte[] bytes, int schemaID) {
      this.bytes = bytes;
      this.schemaID = schemaID;
    }

    public byte[] getBytes() {
      return bytes;
    }

    public int getSchemaID() {
      return schemaID;
    }
  }
}
