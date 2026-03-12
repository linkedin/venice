package com.linkedin.davinci.utils;

import com.linkedin.davinci.storage.chunking.ChunkedValueManifestContainer;
import com.linkedin.davinci.storage.chunking.RawBytesChunkingAdapter;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.RawBytesStoreDeserializerCache;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class ChunkAssembler {
  private static final Logger LOGGER = LogManager.getLogger(ChunkAssembler.class);
  private static final ChunkedValueManifestSerializer RMD_MANIFEST_DESERIALIZER =
      new ChunkedValueManifestSerializer(true);
  protected final StorageEngine bufferStorageEngine;
  private final boolean skipFailedToAssembleRecords;
  private final boolean isRmdChunkingEnabled;

  public ChunkAssembler(StorageEngine bufferStorageEngine, boolean skipFailedToAssembleRecords) {
    this(bufferStorageEngine, skipFailedToAssembleRecords, false);
  }

  public ChunkAssembler(
      StorageEngine bufferStorageEngine,
      boolean skipFailedToAssembleRecords,
      boolean isRmdChunkingEnabled) {
    this.bufferStorageEngine = bufferStorageEngine;
    // disable noisy logs
    this.bufferStorageEngine.suppressLogs(true);
    this.skipFailedToAssembleRecords = skipFailedToAssembleRecords;
    this.isRmdChunkingEnabled = isRmdChunkingEnabled;
  }

  /**
   * Buffers and assembles chunks of a record.
   *
   * If the record is chunked, it stores the chunks and returns null.
   * Once all chunks of a record are received, it returns the compressed and serialized assembled record.
   */
  public ByteBufferValueRecord<ByteBuffer> bufferAndAssembleRecord(
      PubSubTopicPartition pubSubTopicPartition,
      int schemaId,
      byte[] keyBytes,
      ByteBuffer valueBytes,
      PubSubPosition recordOffset,
      VeniceCompressor compressor) {
    return bufferAndAssembleRecord(
        pubSubTopicPartition,
        schemaId,
        keyBytes,
        valueBytes,
        null,
        recordOffset,
        compressor);
  }

  /**
   * Buffers and assembles chunks of a record, including RMD chunks when RMD chunking is enabled.
   *
   * For CHUNK messages:
   *   - If valueBytes is non-empty: this is a value chunk; store valueBytes.
   *   - Else if replicationMetadataPayload is non-empty: this is an RMD chunk; store replicationMetadataPayload.
   * For CHUNKED_VALUE_MANIFEST messages:
   *   - Assembles the value from stored value chunks.
   *   - If isRmdChunkingEnabled, also assembles the RMD from stored RMD chunks using the RMD manifest
   *     in replicationMetadataPayload, and sets it on the returned record.
   */
  public ByteBufferValueRecord<ByteBuffer> bufferAndAssembleRecord(
      PubSubTopicPartition pubSubTopicPartition,
      int schemaId,
      byte[] keyBytes,
      ByteBuffer valueBytes,
      ByteBuffer replicationMetadataPayload,
      PubSubPosition recordOffset,
      VeniceCompressor compressor) {
    ByteBufferValueRecord<ByteBuffer> assembledRecord = null;
    bufferStorageEngine.addStoragePartitionIfAbsent(pubSubTopicPartition.getPartitionNumber());
    // If this is a record chunk, store the chunk and return null for processing this record
    if (schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()) {
      if (valueBytes != null && valueBytes.hasRemaining()) {
        // Value chunk: store value bytes
        bufferStorageEngine.put(
            pubSubTopicPartition.getPartitionNumber(),
            keyBytes,
            ValueRecord.create(schemaId, ByteUtils.extractByteArray(valueBytes)).serialize());
      } else if (isRmdChunkingEnabled && replicationMetadataPayload != null
          && replicationMetadataPayload.hasRemaining()) {
        // RMD chunk: store replicationMetadataPayload bytes
        bufferStorageEngine.put(
            pubSubTopicPartition.getPartitionNumber(),
            keyBytes,
            ValueRecord.create(schemaId, ByteUtils.extractByteArray(replicationMetadataPayload)).serialize());
      }
      return null;
    } else if (schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
      // This is the last value. Store it, and now read it from the in memory store as a fully assembled value
      byte[] manifestByteArray = ByteUtils.extractByteArray(valueBytes);
      bufferStorageEngine.put(
          pubSubTopicPartition.getPartitionNumber(),
          keyBytes,
          ValueRecord.create(schemaId, manifestByteArray).serialize());
      try {
        ChunkedValueManifestContainer manifestContainer = new ChunkedValueManifestContainer();
        ByteBufferValueRecord<ByteBuffer> valueRecord = RawBytesChunkingAdapter.INSTANCE.getWithSchemaId(
            bufferStorageEngine,
            pubSubTopicPartition.getPartitionNumber(),
            ByteBuffer.wrap(keyBytes),
            false,
            null,
            null,
            RawBytesStoreDeserializerCache.getInstance(),
            compressor,
            manifestContainer);

        // Assemble RMD from stored RMD chunks before eviction
        ByteBuffer assembledRmd = null;
        ChunkedValueManifest rmdManifest = null;
        if (isRmdChunkingEnabled && replicationMetadataPayload != null && replicationMetadataPayload.hasRemaining()) {
          rmdManifest = RMD_MANIFEST_DESERIALIZER.deserialize(
              ByteUtils.extractByteArray(replicationMetadataPayload),
              AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion());
          byte[] assembledRmdBytes = new byte[rmdManifest.getSize()];
          int offset = 0;
          for (ByteBuffer rmdChunkKey: rmdManifest.getKeysWithChunkIdSuffix()) {
            byte[] rawRecord = bufferStorageEngine
                .get(pubSubTopicPartition.getPartitionNumber(), ByteUtils.extractByteArray(rmdChunkKey));
            if (rawRecord == null) {
              throw new VeniceException(
                  "Missing RMD chunk for key in topic: " + pubSubTopicPartition.getPubSubTopic().getName());
            }
            int dataLength = rawRecord.length - ValueRecord.SCHEMA_HEADER_LENGTH;
            System.arraycopy(rawRecord, ValueRecord.SCHEMA_HEADER_LENGTH, assembledRmdBytes, offset, dataLength);
            offset += dataLength;
          }
          assembledRmd = ByteBuffer.wrap(assembledRmdBytes);
        }

        evictChunks(pubSubTopicPartition.getPartitionNumber(), keyBytes, manifestContainer, rmdManifest);

        if (valueRecord != null) {
          assembledRecord =
              new ByteBufferValueRecord<>(valueRecord.value(), valueRecord.writerSchemaId(), assembledRmd);
        }
      } catch (Exception ex) {
        // We might get an exception if we haven't persisted all the chunks for a given key. This
        // can actually happen if the client seeks to the middle of a chunked record either by
        // only tailing the records or through direct offset management. This is ok, we just won't
        // return this record since this is a course grained approach we can drop it.
        if (skipFailedToAssembleRecords) {
          LOGGER.warn(
              "Encountered error assembling chunked record, this can happen when seeking between chunked records. Skipping offset {} on topic {}",
              recordOffset,
              pubSubTopicPartition.getPubSubTopic().getName());
        } else {
          throw new VeniceException(
              "Failed to assemble record with offset: " + recordOffset + " on topic: "
                  + pubSubTopicPartition.getPubSubTopic().getName(),
              ex);
        }
      }
    } else {
      // this is a fully specified record, no need to buffer and assemble it, just return the valueBytes
      try {
        assembledRecord = new ByteBufferValueRecord<>(valueBytes, schemaId);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return assembledRecord;
  }

  abstract void evictChunks(
      int partitionId,
      byte[] keyBytes,
      ChunkedValueManifestContainer manifestContainer,
      ChunkedValueManifest rmdManifest);

  public void clearBuffer() {
    bufferStorageEngine.drop();
  }

  public static boolean isChunkedRecord(int schemaId) {
    return schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()
        || schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
  }

  /**
   * For chunked records, the chunk assembler already decompresses the assembled value via
   * RawBytesChunkingAdapter's decompressingInputStreamDecoder. For non-chunked records, the
   * value is still compressed and needs explicit decompression here.
   */
  public static ByteBuffer decompressValueIfNeeded(ByteBuffer value, int schemaId, VeniceCompressor compressor)
      throws IOException {
    return isChunkedRecord(schemaId) ? value : compressor.decompress(value);
  }
}
