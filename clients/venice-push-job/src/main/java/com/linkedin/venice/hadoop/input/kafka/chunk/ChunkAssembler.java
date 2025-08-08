package com.linkedin.venice.hadoop.input.kafka.chunk;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;


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
  private final boolean isRmdChunkingEnabled;

  public ChunkAssembler(boolean isRmdChunkingEnabled) {
    this.isRmdChunkingEnabled = isRmdChunkingEnabled;
    this.manifestSerializer = new ChunkedValueManifestSerializer(true);
  }

  /**
   * The `valueIterator` of this function is supposed to be in descending order by offset.
   *
   * Here is the high-level algo:
   * 1. If the latest event is a `DELETE`, return;
   * 2. If the latest event is a regular `PUT`, return;
   * 3. If the latest event is a manifest, capture all the chunk info from the latest one and ignore the older ones.
   * 4. For chunks:
   *    (a). If there is no manifest captured yet, ignore.
   *    (b). If there is a manifest captured previously, check whether the current chunk belongs to it or not.
   */
  public ValueBytesAndSchemaId assembleAndGetValue(final byte[] keyBytes, final Iterator<byte[]> valueIterator) {
    if (!valueIterator.hasNext()) {
      throw new IllegalArgumentException("Expect values to be not empty.");
    }

    KafkaInputMapperValue reusedMapperValue = null;
    ChunkedValueManifest latestChunkedValueManifest = null;
    int latestChunkedValueManifestRMDVersionId = -1;
    ByteBuffer latestChunkedValueManifestRMDPayload = null;
    ChunkedValueManifest latestChunkedRmdManifest = null;

    PubSubPosition lastOffset = PubSubSymbolicPosition.LATEST;

    byte[][] valueChunks = new byte[0][0];
    ByteBuffer[] valueChunkKeySuffixes = new ByteBuffer[0];
    int valueChunksFound = 0;
    int totalValueByteCount = 0;

    // Below fields are only useful when RMD chunking is enabled.
    byte[][] rmdChunks = new byte[0][0];
    ByteBuffer[] rmdChunkKeySuffixes = new ByteBuffer[0];
    int rmdChunksFound = 0;
    int totalRmdByteCount = 0;

    PubSubPosition reusedMapperValuePosition;
    while (valueIterator.hasNext()) { // Start from the value with the highest offset
      byte[] currentValue = valueIterator.next();
      reusedMapperValue = KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER.deserialize(
          reusedMapperValue,
          OPTIMIZED_BINARY_DECODER_FACTORY.createOptimizedBinaryDecoder(currentValue, 0, currentValue.length));

      reusedMapperValuePosition = PubSubPositionDeserializer.deserializePubSubPosition(
          reusedMapperValue.getPositionWireBytes(),
          reusedMapperValue.getPositionFactoryClass().toString());
      if (PubSubUtil.comparePubSubPositions(reusedMapperValuePosition, lastOffset) > 0) {
        throw new VeniceException(
            "Unexpected, the input is supposed to be in descending order by offset, previous offset: " + lastOffset
                + ", current offset: " + reusedMapperValuePosition);
      }
      lastOffset = reusedMapperValuePosition;

      if (reusedMapperValue.valueType.equals(MapperValueType.DELETE)) {
        if (latestChunkedValueManifest != null) {
          // Ignore older entries since a more recent manifest is discovered.
          continue;
        }
        if (reusedMapperValue.schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()) {
          // Ignore all the chunk cleanup messages.
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
          latestChunkedValueManifest = manifestSerializer.deserialize(
              ByteUtils.extractByteArray(reusedMapperValue.value),
              AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion());
          int valueChunkCount = latestChunkedValueManifest.keysWithChunkIdSuffix.size();
          valueChunks = new byte[valueChunkCount][];
          valueChunkKeySuffixes = new ByteBuffer[valueChunkCount];
          extractKeySuffixForChunks(latestChunkedValueManifest, keyBytes.length, valueChunkKeySuffixes);
          latestChunkedValueManifestRMDVersionId = reusedMapperValue.replicationMetadataVersionId;
          if (!isRmdChunkingEnabled) {
            latestChunkedValueManifestRMDPayload =
                ByteBuffer.wrap(ByteUtils.copyByteArray(reusedMapperValue.replicationMetadataPayload));
          } else {
            latestChunkedRmdManifest = manifestSerializer.deserialize(
                ByteUtils.extractByteArray(reusedMapperValue.replicationMetadataPayload),
                AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion());
            int rmdChunkCount = latestChunkedRmdManifest.keysWithChunkIdSuffix.size();
            rmdChunks = new byte[rmdChunkCount][];
            rmdChunkKeySuffixes = new ByteBuffer[rmdChunkCount];
            extractKeySuffixForChunks(latestChunkedRmdManifest, keyBytes.length, rmdChunkKeySuffixes);
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
        // Matching chunk information with value and RMD chunks.
        boolean isChunkMatched = false;
        if (isRmdChunkingEnabled && (rmdChunksFound != rmdChunkKeySuffixes.length)) {
          for (int i = 0; i < rmdChunkKeySuffixes.length; i++) {
            ByteBuffer byteBuffer = rmdChunkKeySuffixes[i];
            if (byteBuffer.equals(reusedMapperValue.chunkedKeySuffix)) {
              byte[] rmdChunk = new byte[reusedMapperValue.replicationMetadataPayload.remaining()];
              totalRmdByteCount += rmdChunk.length;
              reusedMapperValue.replicationMetadataPayload.get(rmdChunk);
              rmdChunks[i] = rmdChunk;
              rmdChunksFound++;
              isChunkMatched = true;
              break;
            }
          }
        }
        // Bypass iterations on value chunk matching if it is matched with RMD chunk.
        if (!isChunkMatched) {
          for (int i = 0; i < valueChunkKeySuffixes.length; i++) {
            ByteBuffer byteBuffer = valueChunkKeySuffixes[i];
            if (byteBuffer.equals(reusedMapperValue.chunkedKeySuffix)) {
              byte[] valueChunk = new byte[reusedMapperValue.value.remaining()];
              totalValueByteCount += valueChunk.length;
              reusedMapperValue.value.get(valueChunk);
              valueChunks[i] = valueChunk;
              valueChunksFound++;
              break;
            }
          }
        }
        if ((valueChunksFound == valueChunks.length) && (rmdChunksFound == rmdChunks.length)) {
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
      if (valueChunksFound != valueChunks.length) {
        int missingChunks = valueChunks.length - valueChunksFound;
        throw new VeniceException(
            "Cannot assemble a large value. Missing " + missingChunks + " / " + valueChunks.length + " chunks.");
      }
      if (totalValueByteCount != latestChunkedValueManifest.size) {
        throw new VeniceException(
            String
                .format("Expect %d byte(s) but got %d byte(s)", latestChunkedValueManifest.size, totalValueByteCount));
      }
      if (!isRmdChunkingEnabled) {
        return new ValueBytesAndSchemaId(
            concatenateAllChunks(valueChunks, totalValueByteCount),
            latestChunkedValueManifest.schemaId,
            latestChunkedValueManifestRMDVersionId,
            latestChunkedValueManifestRMDPayload);
      }

      if (rmdChunksFound != rmdChunks.length) {
        int missingChunks = rmdChunks.length - rmdChunksFound;
        throw new VeniceException(
            "Cannot assemble a large RMD. Missing " + missingChunks + " / " + rmdChunks.length + " chunks.");
      }
      if (totalRmdByteCount != latestChunkedRmdManifest.size) {
        throw new VeniceException(
            String.format("Expect %d byte(s) but got %d byte(s)", latestChunkedRmdManifest.size, totalRmdByteCount));
      }
      return new ValueBytesAndSchemaId(
          concatenateAllChunks(valueChunks, totalValueByteCount),
          latestChunkedValueManifest.schemaId,
          latestChunkedValueManifestRMDVersionId,
          ByteBuffer.wrap(concatenateAllChunks(rmdChunks, totalRmdByteCount)));

    }
  }

  private void extractKeySuffixForChunks(
      ChunkedValueManifest manifest,
      int keyBytesLength,
      ByteBuffer[] chunkKeySuffixes) {
    for (int i = 0; i < manifest.keysWithChunkIdSuffix.size(); i++) {
      ByteBuffer byteBuffer = manifest.keysWithChunkIdSuffix.get(i);
      int startPosition = byteBuffer.position() + keyBytesLength;
      int suffixLength = byteBuffer.remaining() - keyBytesLength;
      chunkKeySuffixes[i] = ByteBuffer.wrap(byteBuffer.array(), startPosition, suffixLength);
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
    private final int schemaID;
    private final int replicationMetadataVersionId;
    private byte[] bytes;
    private ByteBuffer replicationMetadataPayload;

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

    public void setReplicationMetadataPayload(ByteBuffer replicationMetadataPayload) {
      this.replicationMetadataPayload = replicationMetadataPayload;
    }

    public void setBytes(byte[] bytes) {
      this.bytes = bytes;
    }
  }
}
