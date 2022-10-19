package com.linkedin.venice.hadoop.input.kafka.chunk;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serializer.AvroSpecificDeserializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.io.BytesWritable;


/**
 * This class accumulates all mapper values and assemble them to provide assembled complete large values or regular
 * message.
 */
public class ChunkAssembler {
  private static final RecordDeserializer<KafkaInputMapperValue> KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER =
      FastSerializerDeserializerFactory
          .getFastAvroSpecificDeserializer(KafkaInputMapperValue.SCHEMA$, KafkaInputMapperValue.class);
  private final AvroSpecificDeserializer<ChunkedKeySuffix> chunkedKeySuffixDeserializer;
  private final ChunkedValueManifestSerializer manifestSerializer;

  public ChunkAssembler() {
    SpecificDatumReader<ChunkedKeySuffix> specificDatumReader = new SpecificDatumReader<>(ChunkedKeySuffix.class);
    this.chunkedKeySuffixDeserializer = new AvroSpecificDeserializer<>(specificDatumReader);
    this.manifestSerializer = new ChunkedValueManifestSerializer(true);
  }

  public ValueBytesAndSchemaId assembleAndGetValue(final byte[] keyBytes, final Iterator<BytesWritable> valueIterator) {
    if (!valueIterator.hasNext()) {
      throw new IllegalArgumentException("Expect values to be not empty.");
    }
    // TODO: Getting rid of this large allocation would require leveraging MR secondary sorting
    List<byte[]> chunkBytes = new ArrayList<>();

    KafkaInputMapperValue mapperValue = null;
    byte[] currentValueBytes;
    byte[] largestValueBytes = null;
    long largestOffset = Long.MIN_VALUE;
    while (valueIterator.hasNext()) { // Start from the value with the highest offset
      currentValueBytes = valueIterator.next().copyBytes();
      mapperValue = KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER.deserialize(mapperValue, currentValueBytes);
      if (mapperValue.schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()) {
        chunkBytes.add(currentValueBytes);
      } else if (mapperValue.offset > largestOffset) {
        largestOffset = mapperValue.offset;
        largestValueBytes = currentValueBytes;
      }
    }
    if (largestValueBytes == null) {
      throw new VeniceException("No regular value nor chunk manifest for key: " + ByteUtils.toHexString(keyBytes));
    }
    mapperValue = KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER.deserialize(mapperValue, largestValueBytes);

    if (mapperValue.valueType == MapperValueType.DELETE) {
      return null;
    } else if (mapperValue.schemaId > 0) {
      return new ValueBytesAndSchemaId(mapperValue.value, mapperValue.schemaId);
    } else if (mapperValue.schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
      final ChunkedValueManifest chunkedValueManifest = manifestSerializer.deserialize(
          ByteUtils.extractByteArray(mapperValue.value),
          AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion());

      return assembleLargeValue(keyBytes, chunkedValueManifest, chunkBytes);
    } else {
      throw new VeniceException(
          String.format(
              "Unhandled case with chunked key suffix %s and value schema ID %d",
              deserializeChunkedKeySuffix(mapperValue.chunkedKeySuffix),
              mapperValue.schemaId));
    }
  }

  private ValueBytesAndSchemaId assembleLargeValue(
      byte[] keyBytes,
      final ChunkedValueManifest manifest,
      List<byte[]> allChunkBytes) {
    byte[][] valueChunks = new byte[manifest.keysWithChunkIdSuffix.size()][];
    ByteBuffer[] chunkKeySuffixes = new ByteBuffer[manifest.keysWithChunkIdSuffix.size()];
    int startPosition, suffixLength;
    ByteBuffer byteBuffer;
    for (int i = 0; i < manifest.keysWithChunkIdSuffix.size(); i++) {
      byteBuffer = manifest.keysWithChunkIdSuffix.get(i);
      startPosition = byteBuffer.position() + keyBytes.length;
      suffixLength = byteBuffer.remaining() - keyBytes.length;
      chunkKeySuffixes[i] = ByteBuffer.wrap(byteBuffer.array(), startPosition, suffixLength);
    }

    int totalByteCount = 0;
    KafkaInputMapperValue chunk = null;
    int chunksFound = 0;
    for (byte[] chunkBytes: allChunkBytes) {
      if (chunksFound == valueChunks.length) {
        break;
      }
      chunk = KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER.deserialize(chunk, chunkBytes);
      for (int i = 0; i < chunkKeySuffixes.length; i++) {
        byteBuffer = chunkKeySuffixes[i];
        if (byteBuffer.equals(chunk.chunkedKeySuffix)) {
          byte[] valueChunk = new byte[chunk.value.remaining()];
          totalByteCount += valueChunk.length;
          chunk.value.get(valueChunk);
          valueChunks[i] = valueChunk;
          chunksFound++;
          break;
        }
      }
    }

    if (chunksFound != valueChunks.length) {
      int missingChunks = valueChunks.length - chunksFound;
      throw new VeniceException(
          "Cannot assemble a large value. Missing " + missingChunks + " / " + valueChunks.length + " chunks.");
    }

    if (totalByteCount != manifest.size) {
      throw new VeniceException(String.format("Expect %d byte(s) but got %d byte(s)", manifest.size, totalByteCount));
    }
    return new ValueBytesAndSchemaId(concatenateAllChunks(valueChunks, totalByteCount), manifest.schemaId);
  }

  private ChunkedKeySuffix deserializeChunkedKeySuffix(ByteBuffer chunkedKeySuffixBytes) {
    return chunkedKeySuffixDeserializer.deserialize(chunkedKeySuffixBytes);
  }

  private byte[] concatenateAllChunks(final byte[][] chunks, final int totalByteCount) {
    byte[] concatenatedChunk = new byte[totalByteCount];
    int currStartingIndexInDst = 0;
    for (byte[] chunk: chunks) {
      System.arraycopy(chunk, 0, concatenatedChunk, currStartingIndexInDst, chunk.length);
      currStartingIndexInDst += chunk.length;
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
