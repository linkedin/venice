package com.linkedin.davinci.utils;

import com.linkedin.davinci.storage.chunking.ChunkedValueManifestContainer;
import com.linkedin.davinci.storage.chunking.RawBytesChunkingAdapter;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.RawBytesStoreDeserializerCache;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class ChunkAssembler {
  private static final Logger LOGGER = LogManager.getLogger(ChunkAssembler.class);
  protected final StorageEngine bufferStorageEngine;
  private final boolean skipFailedToAssembleRecords;

  public ChunkAssembler(StorageEngine bufferStorageEngine, boolean skipFailedToAssembleRecords) {
    this.bufferStorageEngine = bufferStorageEngine;
    // disable noisy logs
    this.bufferStorageEngine.suppressLogs(true);
    this.skipFailedToAssembleRecords = skipFailedToAssembleRecords;
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
      long recordOffset,
      VeniceCompressor compressor) {
    ByteBufferValueRecord<ByteBuffer> assembledRecord = null;
    bufferStorageEngine.addStoragePartitionIfAbsent(pubSubTopicPartition.getPartitionNumber());
    // If this is a record chunk, store the chunk and return null for processing this record
    if (schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()) {
      bufferStorageEngine.put(
          pubSubTopicPartition.getPartitionNumber(),
          keyBytes,
          ValueRecord.create(schemaId, ByteUtils.extractByteArray(valueBytes)).serialize());
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
        assembledRecord = RawBytesChunkingAdapter.INSTANCE.getWithSchemaId(
            bufferStorageEngine,
            pubSubTopicPartition.getPartitionNumber(),
            ByteBuffer.wrap(keyBytes),
            false,
            null,
            null,
            RawBytesStoreDeserializerCache.getInstance(),
            compressor,
            manifestContainer);
        evictChunks(pubSubTopicPartition.getPartitionNumber(), keyBytes, manifestContainer);
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

  abstract void evictChunks(int partitionId, byte[] keyBytes, ChunkedValueManifestContainer manifestContainer);

  public void clearBuffer() {
    bufferStorageEngine.drop();
  }

  public static boolean isChunkedRecord(int schemaId) {
    return schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()
        || schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
  }
}
