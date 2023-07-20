package com.linkedin.venice.writer;

import static com.linkedin.venice.writer.VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.storage.protocol.ChunkId;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.BiConsumer;
import java.util.function.Supplier;


/**
 * This class is a helper class that contains writer side chunking logics.
 */
public class WriterChunkingHelper {
  public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

  /**
   * This method chunks payload and send each chunk out.
   * @param serializedKey serialized key input
   * @param payload serialized payload could be value bytes or RMD bytes.
   * @param schemaId value schema ID
   * @param isChunkAwareCallback boolean flag indicating whether to create chunk
   * @param sizeReport supplier function for size report.
   * @param maxSizeForUserPayloadPerMessageInBytes maximum size for payload in a message
   * @param keyWithChunkingSuffixSerializer Chuncking suffix serializer for key
   * @param sendMessageFunction Pass in function for sending message
   * @return Chunked payload arrays and manifest.
   */
  public static ChunkedPayloadAndManifest chunkPayloadAndSend(
      byte[] serializedKey,
      byte[] payload,
      boolean isValuePayload,
      int schemaId,
      int chunkedKeySuffixStartingIndex,
      boolean isChunkAwareCallback,
      Supplier<String> sizeReport,
      int maxSizeForUserPayloadPerMessageInBytes,
      KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer,
      BiConsumer<VeniceWriter.KeyProvider, Put> sendMessageFunction) {
    int sizeAvailablePerMessage = maxSizeForUserPayloadPerMessageInBytes - serializedKey.length;
    validateAvailableSizePerMessage(maxSizeForUserPayloadPerMessageInBytes, sizeAvailablePerMessage, sizeReport);
    int numberOfChunks = (int) Math.ceil((double) payload.length / (double) sizeAvailablePerMessage);
    final ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.schemaId = schemaId;
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(numberOfChunks);
    chunkedValueManifest.size = payload.length;

    VeniceWriter.KeyProvider keyProvider, firstKeyProvider, subsequentKeyProvider;
    ByteBuffer[] chunks = null;
    if (isChunkAwareCallback) {
      // We only carry this state if it's going to be used, else we don't even instantiate this
      chunks = new ByteBuffer[numberOfChunks];
    }

    /**
     * This {@link ChunkedKeySuffix} instance gets mutated along the way, first in the loop, where its
     * chunk index is incremented at each iteration, and then also on the first iteration of the loop,
     * the {@link firstKeyProvider} will extract {@link ProducerMetadata} information out of the first
     * message sent, to be re-used across all chunk keys belonging to this value.
     */
    final ChunkedKeySuffix chunkedKeySuffix = new ChunkedKeySuffix();
    chunkedKeySuffix.isChunk = true;
    chunkedKeySuffix.chunkId = new ChunkId();

    subsequentKeyProvider = producerMetadata -> {
      ByteBuffer keyWithSuffix = keyWithChunkingSuffixSerializer.serializeChunkedKey(serializedKey, chunkedKeySuffix);
      chunkedValueManifest.keysWithChunkIdSuffix.add(keyWithSuffix);
      return new KafkaKey(MessageType.PUT, keyWithSuffix.array());
    };
    firstKeyProvider = producerMetadata -> {
      chunkedKeySuffix.chunkId.producerGUID = producerMetadata.producerGUID;
      chunkedKeySuffix.chunkId.segmentNumber = producerMetadata.segmentNumber;
      chunkedKeySuffix.chunkId.messageSequenceNumber = producerMetadata.messageSequenceNumber;
      return subsequentKeyProvider.getKey(producerMetadata);
    };
    for (int chunkIndex = 0; chunkIndex < numberOfChunks; chunkIndex++) {
      int chunkStartByteIndex = chunkIndex * sizeAvailablePerMessage;
      int chunkEndByteIndex = Math.min((chunkIndex + 1) * sizeAvailablePerMessage, payload.length);

      /**
       * We leave 4 bytes of headroom at the beginning of the ByteBuffer so that the Venice Storage Node
       * can use this room to write the value header, without allocating a new byte array nor copying.
       */
      final int chunkLength = chunkEndByteIndex - chunkStartByteIndex;
      byte[] chunkValue = new byte[chunkLength + ByteUtils.SIZE_OF_INT];
      System.arraycopy(payload, chunkStartByteIndex, chunkValue, ByteUtils.SIZE_OF_INT, chunkLength);
      ByteBuffer chunk = ByteBuffer.wrap(chunkValue);
      chunk.position(ByteUtils.SIZE_OF_INT);

      if (chunks != null) {
        chunks[chunkIndex] = chunk;
      }

      Put putPayload = new Put();
      putPayload.schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
      putPayload.replicationMetadataVersionId = VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
      if (isValuePayload) {
        putPayload.putValue = chunk;
        putPayload.replicationMetadataPayload = EMPTY_BYTE_BUFFER;
      } else {
        putPayload.putValue = EMPTY_BYTE_BUFFER;
        putPayload.replicationMetadataPayload = chunk;
      }
      chunkedKeySuffix.chunkId.chunkIndex = chunkIndex + chunkedKeySuffixStartingIndex;
      keyProvider = chunkIndex == 0 ? firstKeyProvider : subsequentKeyProvider;

      try {
        /** Non-blocking */
        sendMessageFunction.accept(keyProvider, putPayload);
      } catch (Exception e) {
        throw new VeniceException(
            "Caught an exception while attempting to produce a chunk of a large value into Kafka... "
                + getDetailedSizeReport(chunkIndex, numberOfChunks, sizeAvailablePerMessage, sizeReport),
            e);
      }
    }
    return new ChunkedPayloadAndManifest(chunks, chunkedValueManifest);
  }

  private static void validateAvailableSizePerMessage(
      int maxSizeForUserPayloadPerMessageInBytes,
      int sizeAvailablePerMessage,
      Supplier<String> sizeReport) {
    if (sizeAvailablePerMessage < maxSizeForUserPayloadPerMessageInBytes / 2) {
      /**
       * If the key size is more than half the available payload per message, then we cannot even encode a
       * {@link ChunkedValueManifest} with two keys in it, hence we would fail anyway. Might as well fail fast.
       *
       * N.B.: The above may not be strictly true, since Kafka-level compression may squeeze us under the limit,
       * but this is not likely to work deterministically, so it would be iffy to attempt it on a best-effort
       * basis. That being said, allowing keys to be as large as half the available payload size is probably
       * overly permissive anyway. Ideally, the key size should be much smaller than this.
       *
       * TODO: Implement a proper key size (and value size) quota mechanism.
       */
      throw new VeniceException("Chunking cannot support this use case. The key is too large. " + sizeReport.get());
    }
  }

  private static String getDetailedSizeReport(
      int chunkIndex,
      int numberOfChunks,
      int sizeAvailablePerMessage,
      Supplier<String> sizeReport) {
    return String.format(
        "Current chunk index: %d, Number of chunks: %d, Size available per message: %d, %s",
        chunkIndex,
        numberOfChunks,
        sizeAvailablePerMessage,
        sizeReport.get());
  }
}
