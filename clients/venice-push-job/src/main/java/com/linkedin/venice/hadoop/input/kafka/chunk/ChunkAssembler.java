package com.linkedin.venice.hadoop.input.kafka.chunk;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;
import org.apache.hadoop.io.BytesWritable;


/**
 * This class accumulates all mapper values and assemble them to provide assembled complete large values or regular
 * message.
 */
public class ChunkAssembler {
  private static final OptimizedBinaryDecoderFactory OPTIMIZED_BINARY_DECODER_FACTORY =
      OptimizedBinaryDecoderFactory.defaultFactory();
  private static final RecordDeserializer<KafkaInputMapperValue> KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER =
      FastSerializerDeserializerFactory
          .getFastAvroSpecificDeserializer(KafkaInputMapperValue.SCHEMA$, KafkaInputMapperValue.class);
  private final ChunkedValueManifestSerializer manifestSerializer;

  public ChunkAssembler() {
    this.manifestSerializer = new ChunkedValueManifestSerializer(true);
  }

  /**
   * The `valueIterator` of this function is supposed to be in descending order by offset.
   *
   * Here is the high-level algo:
   * 1. If the latest event is a `DELETE`, return;
   * 2. If the latest event is a regular 'PUT`, return;
   * 3. If the latest event is a manifest, capture all the chunk info from the latest one and ignore the older ones.
   * 4. For chunks:
   *    a. If there is no manifest captured yet, ignore.
   *    b. If there is a manifest captured previously, check whether the current chunk belongs to it or not.
   */
  public ValueBytesAndSchemaId assembleAndGetValue(final byte[] keyBytes, final Iterator<BytesWritable> valueIterator) {
    if (!valueIterator.hasNext()) {
      throw new IllegalArgumentException("Expect values to be not empty.");
    }

    KafkaInputMapperValue reusedMapperValue = null;

    ChunkedValueManifest latestChunkedValueManifest = null;
    int latestChunkedValueManifestRMDVersionId = -1;
    ByteBuffer latestChunkedValueManifestRMDPayload = null;
    byte[][] valueChunks = new byte[0][0];
    ByteBuffer[] chunkKeySuffixes = new ByteBuffer[0];
    int chunksFound = 0;
    int totalByteCount = 0;
    long lastOffset = Long.MAX_VALUE;

    while (valueIterator.hasNext()) { // Start from the value with the highest offset
      BytesWritable currentValue = valueIterator.next();
      reusedMapperValue = KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER.deserialize(
          reusedMapperValue,
          OPTIMIZED_BINARY_DECODER_FACTORY
              .createOptimizedBinaryDecoder(currentValue.getBytes(), 0, currentValue.getLength()));
      if (reusedMapperValue.offset > lastOffset) {
        throw new VeniceException(
            "Unexpected, the input is supposed to be in descending order by offset, previous offset: " + lastOffset
                + ", current offset: " + reusedMapperValue.offset);
      }
      lastOffset = reusedMapperValue.offset;

      if (reusedMapperValue.valueType.equals(MapperValueType.DELETE)) {
        if (latestChunkedValueManifest != null) {
          // Ignore older entries since a more recent manifest is discovered.
          continue;
        }
        /**
         * The latest event is a delete event.
         */
        if (reusedMapperValue.replicationMetadataPayload.remaining() != 0) {
          return new ValueBytesAndSchemaId(
              null,
              reusedMapperValue.schemaId,
              reusedMapperValue.replicationMetadataVersionId,
              reusedMapperValue.replicationMetadataPayload);
        }
        return null;
      }

      if (reusedMapperValue.schemaId > 0) {
        if (latestChunkedValueManifest != null) {
          // Ignore older entries since a more recent manifest is discovered.
          continue;
        }
        /**
         * The latest event is a put event.
         */
        return new ValueBytesAndSchemaId(
            ByteUtils.extractByteArray(reusedMapperValue.value),
            reusedMapperValue.schemaId,
            reusedMapperValue.replicationMetadataVersionId,
            reusedMapperValue.replicationMetadataPayload);
      }

      if (reusedMapperValue.schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
        // Only capture the latest manifest
        if (latestChunkedValueManifest == null) {
          latestChunkedValueManifestRMDVersionId = reusedMapperValue.replicationMetadataVersionId;
          latestChunkedValueManifestRMDPayload =
              ByteBuffer.wrap(ByteUtils.copyByteArray(reusedMapperValue.replicationMetadataPayload));
          latestChunkedValueManifest = manifestSerializer.deserialize(
              ByteUtils.extractByteArray(reusedMapperValue.value),
              AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion());
          int chunkCount = latestChunkedValueManifest.keysWithChunkIdSuffix.size();
          valueChunks = new byte[chunkCount][];
          chunkKeySuffixes = new ByteBuffer[chunkCount];

          for (int i = 0; i < latestChunkedValueManifest.keysWithChunkIdSuffix.size(); i++) {
            ByteBuffer byteBuffer = latestChunkedValueManifest.keysWithChunkIdSuffix.get(i);
            int startPosition = byteBuffer.position() + keyBytes.length;
            int suffixLength = byteBuffer.remaining() - keyBytes.length;
            chunkKeySuffixes[i] = ByteBuffer.wrap(byteBuffer.array(), startPosition, suffixLength);
          }
        }
      } else if (reusedMapperValue.schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()) {
        if (latestChunkedValueManifest == null) {
          /**
           * It is possible that KafkaInputFormat could only read the partial chunks for a large message, which means
           * there are chunks without the corresponding manifest.
           */
          continue;
        }
        // Collecting chunks
        for (int i = 0; i < chunkKeySuffixes.length; i++) {
          ByteBuffer byteBuffer = chunkKeySuffixes[i];
          if (byteBuffer.equals(reusedMapperValue.chunkedKeySuffix)) {
            byte[] valueChunk = new byte[reusedMapperValue.value.remaining()];
            totalByteCount += valueChunk.length;
            reusedMapperValue.value.get(valueChunk);
            valueChunks[i] = valueChunk;
            chunksFound++;
            break;
          }
        }
        if (chunksFound == valueChunks.length) {
          break;
        }
      } else {
        throw new IllegalArgumentException("Unexpected schema id: " + reusedMapperValue.schemaId);
      }
    }
    if (latestChunkedValueManifest == null) {
      // No valid data.
      return null;
    } else {
      if (chunksFound != valueChunks.length) {
        int missingChunks = valueChunks.length - chunksFound;
        throw new VeniceException(
            "Cannot assemble a large value. Missing " + missingChunks + " / " + valueChunks.length + " chunks.");
      }
      if (totalByteCount != latestChunkedValueManifest.size) {
        throw new VeniceException(
            String.format("Expect %d byte(s) but got %d byte(s)", latestChunkedValueManifest.size, totalByteCount));
      }
      return new ValueBytesAndSchemaId(
          concatenateAllChunks(valueChunks, totalByteCount),
          latestChunkedValueManifest.schemaId,
          latestChunkedValueManifestRMDVersionId,
          latestChunkedValueManifestRMDPayload);
    }
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

    private final int replicationMetadataVersionId;
    private final ByteBuffer replicationMetadataPayload;

    ValueBytesAndSchemaId(byte[] bytes, int schemaID, int rmdId, ByteBuffer rmdPayload) {
      this.bytes = bytes;
      this.schemaID = schemaID;
      this.replicationMetadataVersionId = rmdId;
      this.replicationMetadataPayload = rmdPayload;
    }

    public byte[] getBytes() {
      return bytes;
    }

    public int getSchemaID() {
      return schemaID;
    }

    public int getReplicationMetadataVersionId() {
      return replicationMetadataVersionId;
    }

    public ByteBuffer getReplicationMetadataPayload() {
      return replicationMetadataPayload;
    }
  }
}
