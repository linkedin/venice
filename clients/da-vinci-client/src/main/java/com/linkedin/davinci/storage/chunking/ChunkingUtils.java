package com.linkedin.davinci.storage.chunking;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.StoreDeserializerCache;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;


/**
 * This class and the rest of this package encapsulate the complexity of assembling chunked values
 * from the storage engine. At a high level, value chunking in Venice works this way:
 *
 * The {@link VeniceWriter} performs the chunking, and the ingestion code completely ignores it,
 * treating chunks and full values exactly the same way. Re-assembly then happens at read time.
 *
 * The reason the above strategy works is that when a store-version has chunking enabled, there
 * is a {@link ChunkedKeySuffix} appended to the end of every key. This suffix indicates, via
 * {@link ChunkedKeySuffix#isChunk}, whether the corresponding value is a chunk or a "top-level"
 * key. The suffix is carefully designed to achieve the following goals:
 *
 * 1. Chunks and top-level keys should never collide, so that the storage engine and Kafka log
 *    compaction never inadvertently overwrite a chunk with a top-level key or vice-versa.
 * 2. Byte ordering is preserved assuming the {@link VeniceWriter} writes chunks in order and
 *    then writes the top-level key/value at the end. This is important because Venice is optimized
 *    for ordered ingestion.
 *
 * A top-level key can correspond either to a full value, or to a {@link ChunkedValueManifest}.
 * This is disambiguated by looking at the {@link Put#schemaId} field, which is set to a specific
 * negative value in the case of manifests.
 *
 * @see AvroProtocolDefinition#CHUNKED_VALUE_MANIFEST for the specific ID
 *
 * Therefore, at read time, the following steps are executed:
 *
 * 1. The top-level key is queried.
 * 2. The top-level key's value's schema ID is checked.
 *    a) If it is positive, then it's a full value, and is returned immediately.
 *    b) If it is negative, then it's a {@link ChunkedValueManifest}, and we continue to the next steps.
 * 3. The {@link ChunkedValueManifest} is deserialized, and its chunk keys are extracted.
 * 4. Each chunk key is queried.
 * 5. The chunks are stitched back together using the various adpater interfaces of this package,
 *    depending on whether it is the single get or batch get/compute path that needs to re-assembe
 *    a chunked value.
 */
public class ChunkingUtils {
  static final ChunkedValueManifestSerializer CHUNKED_VALUE_MANIFEST_SERIALIZER =
      new ChunkedValueManifestSerializer(false);
  public static final KeyWithChunkingSuffixSerializer KEY_WITH_CHUNKING_SUFFIX_SERIALIZER =
      new KeyWithChunkingSuffixSerializer();

  /**
   * Fills in default values for the unused parameters of the single get and batch get paths.
   */
  static <VALUE, ASSEMBLED_VALUE_CONTAINER> VALUE getFromStorage(
      ChunkingAdapter<ASSEMBLED_VALUE_CONTAINER, VALUE> adapter,
      AbstractStorageEngine store,
      int partition,
      ByteBuffer keyBuffer,
      ReadResponse response) {
    return getFromStorage(adapter, store, partition, keyBuffer, response, null, null, -1, null, null, false, null);
  }

  static <VALUE, ASSEMBLED_VALUE_CONTAINER> VALUE getReplicationMetadataFromStorage(
      ChunkingAdapter<ASSEMBLED_VALUE_CONTAINER, VALUE> adapter,
      AbstractStorageEngine store,
      int partition,
      ByteBuffer keyBuffer,
      ReadResponse response,
      ChunkedValueManifestContainer manifestContainer) {
    return getFromStorage(
        adapter,
        store,
        partition,
        keyBuffer,
        response,
        null,
        null,
        -1,
        null,
        null,
        true,
        manifestContainer);
  }

  static <VALUE, CHUNKS_CONTAINER> VALUE getFromStorage(
      ChunkingAdapter<CHUNKS_CONTAINER, VALUE> adapter,
      AbstractStorageEngine store,
      int partition,
      byte[] keyBuffer,
      ByteBuffer reusedRawValue,
      VALUE reusedValue,
      BinaryDecoder reusedDecoder,
      ReadResponse response,
      int readerSchemaId,
      StoreDeserializerCache<VALUE> storeDeserializerCache,
      VeniceCompressor compressor) {
    long databaseLookupStartTimeInNS = (response != null) ? System.nanoTime() : 0;
    reusedRawValue = store.get(partition, keyBuffer, reusedRawValue);
    if (reusedRawValue == null) {
      return null;
    }
    return getFromStorage(
        reusedRawValue.array(),
        reusedRawValue.limit(),
        databaseLookupStartTimeInNS,
        adapter,
        store,
        partition,
        response,
        reusedValue,
        reusedDecoder,
        readerSchemaId,
        storeDeserializerCache,
        compressor,
        false,
        null);
  }

  static <CHUNKS_CONTAINER, VALUE> void getFromStorageByPartialKey(
      ChunkingAdapter<CHUNKS_CONTAINER, VALUE> adapter,
      AbstractStorageEngine store,
      int partition,
      byte[] keyPrefixBytes,
      VALUE reusedValue,
      RecordDeserializer<GenericRecord> keyRecordDeserializer,
      BinaryDecoder reusedDecoder,
      ReadResponse response,
      int readerSchemaId,
      StoreDeserializerCache<VALUE> storeDeserializerCache,
      VeniceCompressor compressor,
      StreamingCallback<GenericRecord, GenericRecord> computingCallback) {

    long databaseLookupStartTimeInNS = (response != null) ? System.nanoTime() : 0;

    BytesStreamingCallback callback = new BytesStreamingCallback() {
      GenericRecord deserializedValueRecord;

      @Override
      public void onRecordReceived(byte[] key, byte[] value) {
        if (key == null || value == null) {
          return;
        }

        int writerSchemaId = ValueRecord.parseSchemaId(value);

        if (writerSchemaId > 0) {
          // User-defined schema, thus not a chunked value.

          if (response != null) {
            response.addDatabaseLookupLatency(LatencyUtils.getLatencyInMS(databaseLookupStartTimeInNS));
          }

          GenericRecord deserializedKey = keyRecordDeserializer.deserialize(key);

          deserializedValueRecord = (GenericRecord) adapter.constructValue(
              value,
              value.length,
              reusedValue,
              reusedDecoder,
              response,
              writerSchemaId,
              readerSchemaId,
              storeDeserializerCache,
              compressor);

          computingCallback.onRecordReceived(deserializedKey, deserializedValueRecord);
        } else if (writerSchemaId != AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
          throw new VeniceException("Found a record with invalid schema ID: " + writerSchemaId);
        } else {
          throw new VeniceException("Filtering by key prefix is not supported when chunking is enabled.");
        }
      }

      @Override
      public void onCompletion() {
        /* Nothing to do here. */
      }
    };

    store.getByKeyPrefix(partition, keyPrefixBytes, callback);
  }

  /**
   * Fetches the value associated with the given key, and potentially re-assembles it, if it is
   * a chunked value.
   *
   * This code makes use of the {@link ChunkingAdapter} interface in order to abstract away the
   * different needs of the single get, batch get and compute code paths. This function should
   * not be called directly, from the query code, as it expects the key to be properly formatted
   * already. Use of one these simpler functions instead:
   *
   * @see SingleGetChunkingAdapter#get(AbstractStorageEngine, int, byte[], boolean, ReadResponse)
   * @see BatchGetChunkingAdapter#get(AbstractStorageEngine, int, ByteBuffer, boolean, ReadResponse)
   */
  static <VALUE, CHUNKS_CONTAINER> VALUE getFromStorage(
      ChunkingAdapter<CHUNKS_CONTAINER, VALUE> adapter,
      AbstractStorageEngine store,
      int partition,
      ByteBuffer keyBuffer,
      ReadResponse response,
      VALUE reusedValue,
      BinaryDecoder reusedDecoder,
      int readerSchemaID,
      StoreDeserializerCache<VALUE> storeDeserializerCache,
      VeniceCompressor compressor,
      boolean isRmdValue,
      ChunkedValueManifestContainer manifestContainer) {
    long databaseLookupStartTimeInNS = (response != null) ? System.nanoTime() : 0;
    byte[] value =
        isRmdValue ? store.getReplicationMetadata(partition, keyBuffer.array()) : store.get(partition, keyBuffer);

    return getFromStorage(
        value,
        (value == null ? 0 : value.length),
        databaseLookupStartTimeInNS,
        adapter,
        store,
        partition,
        response,
        reusedValue,
        reusedDecoder,
        readerSchemaID,
        storeDeserializerCache,
        compressor,
        isRmdValue,
        manifestContainer);
  }

  public static ChunkedValueManifest getChunkValueManifestFromStorage(
      byte[] key,
      int partition,
      boolean isRmd,
      AbstractStorageEngine store) {
    byte[] value = isRmd ? store.getReplicationMetadata(partition, key) : store.get(partition, key);
    if (value == null) {
      return null;
    }
    int writerSchemaId = ValueRecord.parseSchemaId(value);
    if (writerSchemaId > 0) {
      return null;
    }
    return CHUNKED_VALUE_MANIFEST_SERIALIZER.deserialize(value, writerSchemaId);
  }

  /**
   * Fetches the value associated with the given key, and potentially re-assembles it, if it is
   * a chunked value.
   *
   * This code makes use of the {@link ChunkingAdapter} interface in order to abstract away the
   * different needs of the single get, batch get and compute code paths. This function should
   * not be called directly, from the query code, as it expects the key to be properly formatted
   * already. Use of one these simpler functions instead:
   *
   * @see SingleGetChunkingAdapter#get(AbstractStorageEngine, int, byte[], boolean, ReadResponse)
   * @see BatchGetChunkingAdapter#get(AbstractStorageEngine, int, ByteBuffer, boolean, ReadResponse)
   */
  private static <VALUE, CHUNKS_CONTAINER> VALUE getFromStorage(
      byte[] value,
      int valueLength,
      long databaseLookupStartTimeInNS,
      ChunkingAdapter<CHUNKS_CONTAINER, VALUE> adapter,
      AbstractStorageEngine store,
      int partition,
      ReadResponse response,
      VALUE reusedValue,
      BinaryDecoder reusedDecoder,
      int readerSchemaId,
      StoreDeserializerCache<VALUE> storeDeserializerCache,
      VeniceCompressor compressor,
      boolean isRmdValue,
      ChunkedValueManifestContainer manifestContainer) {

    if (value == null) {
      return null;
    }
    int writerSchemaId = ValueRecord.parseSchemaId(value);

    if (writerSchemaId > 0) {
      // User-defined schema, thus not a chunked value. Early termination.

      if (response != null) {
        response.addDatabaseLookupLatency(LatencyUtils.getLatencyInMS(databaseLookupStartTimeInNS));
        response.addValueSize(valueLength);
      }
      return adapter.constructValue(
          value,
          valueLength,
          reusedValue,
          reusedDecoder,
          response,
          writerSchemaId,
          readerSchemaId,
          storeDeserializerCache,
          compressor);
    } else if (writerSchemaId != AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
      throw new VeniceException("Found a record with invalid schema ID: " + writerSchemaId);
    }

    // End of initial sanity checks. We have a chunked value, so we need to fetch all chunks

    ChunkedValueManifest chunkedValueManifest = CHUNKED_VALUE_MANIFEST_SERIALIZER.deserialize(value, writerSchemaId);
    if (manifestContainer != null) {
      manifestContainer.setManifest(chunkedValueManifest);
    }
    CHUNKS_CONTAINER assembledValueContainer = adapter.constructChunksContainer(chunkedValueManifest);
    int actualSize = 0;

    for (int chunkIndex = 0; chunkIndex < chunkedValueManifest.keysWithChunkIdSuffix.size(); chunkIndex++) {
      // N.B.: This is done sequentially. Originally, each chunk was fetched concurrently in the same executor
      // as the main queries, but this might cause deadlocks, so we are now doing it sequentially. If we want to
      // optimize large value retrieval in the future, it's unclear whether the concurrent retrieval approach
      // is optimal (as opposed to streaming the response out incrementally, for example). Since this is a
      // premature optimization, we are not addressing it right now.
      byte[] chunkKey = chunkedValueManifest.keysWithChunkIdSuffix.get(chunkIndex).array();
      byte[] valueChunk =
          isRmdValue ? store.getReplicationMetadata(partition, chunkKey) : store.get(partition, chunkKey);

      if (valueChunk == null) {
        throw new VeniceException("Chunk not found in " + getExceptionMessageDetails(store, partition, chunkIndex));
      } else if (ValueRecord.parseSchemaId(valueChunk) != AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()) {
        throw new VeniceException(
            "Did not get the chunk schema ID while attempting to retrieve a chunk! " + "Instead, got schema ID: "
                + ValueRecord.parseSchemaId(valueChunk) + " from "
                + getExceptionMessageDetails(store, partition, chunkIndex));
      }

      actualSize += valueChunk.length - ValueRecord.SCHEMA_HEADER_LENGTH;
      adapter.addChunkIntoContainer(assembledValueContainer, chunkIndex, valueChunk);
    }

    // Sanity check based on size...
    if (actualSize != chunkedValueManifest.size) {
      throw new VeniceException(
          "The fully assembled large value does not have the expected size! " + "actualSize: " + actualSize
              + ", chunkedValueManifest.size: " + chunkedValueManifest.size + ", "
              + getExceptionMessageDetails(store, partition, null));
    }

    if (response != null) {
      response.addDatabaseLookupLatency(LatencyUtils.getLatencyInMS(databaseLookupStartTimeInNS));
      response.addValueSize(actualSize);
      response.incrementMultiChunkLargeValueCount();
    }

    return adapter.constructValue(
        assembledValueContainer,
        reusedValue,
        reusedDecoder,
        response,
        chunkedValueManifest.schemaId,
        readerSchemaId,
        storeDeserializerCache,
        compressor);
  }

  private static String getExceptionMessageDetails(AbstractStorageEngine store, int partition, Integer chunkIndex) {
    String message = "store: " + store.getStoreName() + ", partition: " + partition;
    if (chunkIndex != null) {
      message += ", chunk index: " + chunkIndex;
    }
    message += ".";
    return message;
  }
}
