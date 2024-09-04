package com.linkedin.davinci.storage.chunking;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.davinci.listener.response.ReadResponseStats;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
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
import java.util.Objects;
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
 * 5. The chunks are stitched back together using the various adapter interfaces of this package,
 *    depending on whether it is the single get or batch get/compute path that needs to re-assemble
 *    a chunked value.
 */
public class ChunkingUtils {
  static final ChunkedValueManifestSerializer CHUNKED_VALUE_MANIFEST_SERIALIZER =
      new ChunkedValueManifestSerializer(false);
  public static final KeyWithChunkingSuffixSerializer KEY_WITH_CHUNKING_SUFFIX_SERIALIZER =
      new KeyWithChunkingSuffixSerializer();

  interface StorageGetFunction {
    byte[] apply(int partition, ByteBuffer key);
  }

  /**
   * Fills in default values for the unused parameters of the single get and batch get paths.
   */
  static <VALUE, ASSEMBLED_VALUE_CONTAINER> VALUE getFromStorage(
      ChunkingAdapter<ASSEMBLED_VALUE_CONTAINER, VALUE> adapter,
      AbstractStorageEngine store,
      int partition,
      ByteBuffer keyBuffer,
      ReadResponseStats response) {
    return getFromStorage(
        adapter,
        store::get,
        store.getStoreVersionName(),
        partition,
        keyBuffer,
        response,
        null,
        null,
        -1,
        null,
        null,
        null);
  }

  static <VALUE, ASSEMBLED_VALUE_CONTAINER> VALUE getReplicationMetadataFromStorage(
      ChunkingAdapter<ASSEMBLED_VALUE_CONTAINER, VALUE> adapter,
      AbstractStorageEngine store,
      int partition,
      ByteBuffer keyBuffer,
      ReadResponseStats response,
      ChunkedValueManifestContainer manifestContainer) {
    return getFromStorage(
        adapter,
        store::getReplicationMetadata,
        store.getStoreVersionName(),
        partition,
        keyBuffer,
        response,
        null,
        null,
        -1,
        null,
        null,
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
      ReadResponseStats response,
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
        store::get,
        store.getStoreVersionName(),
        partition,
        response,
        reusedValue,
        reusedDecoder,
        readerSchemaId,
        storeDeserializerCache,
        compressor,
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
      ReadResponseStats response,
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
            response.addDatabaseLookupLatency(LatencyUtils.getElapsedTimeFromNSToMS(databaseLookupStartTimeInNS));
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
   * @see SingleGetChunkingAdapter#get(AbstractStorageEngine, int, byte[], boolean, ReadResponseStats)
   * @see BatchGetChunkingAdapter#get(AbstractStorageEngine, int, ByteBuffer, boolean, ReadResponseStats)
   */
  static <VALUE, CHUNKS_CONTAINER> VALUE getFromStorage(
      ChunkingAdapter<CHUNKS_CONTAINER, VALUE> adapter,
      StorageGetFunction storageGetFunction,
      String storeVersionName,
      int partition,
      ByteBuffer keyBuffer,
      ReadResponseStats response,
      VALUE reusedValue,
      BinaryDecoder reusedDecoder,
      int readerSchemaID,
      StoreDeserializerCache<VALUE> storeDeserializerCache,
      VeniceCompressor compressor,
      ChunkedValueManifestContainer manifestContainer) {
    long databaseLookupStartTimeInNS = (response != null) ? System.nanoTime() : 0;
    byte[] value = storageGetFunction.apply(partition, keyBuffer);

    return getFromStorage(
        value,
        (value == null ? 0 : value.length),
        databaseLookupStartTimeInNS,
        adapter,
        storageGetFunction,
        storeVersionName,
        partition,
        response,
        reusedValue,
        reusedDecoder,
        readerSchemaID,
        storeDeserializerCache,
        compressor,
        manifestContainer);
  }

  static <VALUE, CHUNKS_CONTAINER> ByteBufferValueRecord<VALUE> getValueAndSchemaIdFromStorage(
      ChunkingAdapter<CHUNKS_CONTAINER, VALUE> adapter,
      AbstractStorageEngine store,
      int partition,
      ByteBuffer keyBuffer,
      VALUE reusedValue,
      BinaryDecoder reusedDecoder,
      StoreDeserializerCache<VALUE> storeDeserializerCache,
      VeniceCompressor compressor,
      ChunkedValueManifestContainer manifestContainer) {
    byte[] value = store.get(partition, keyBuffer);
    int writerSchemaId = value == null ? 0 : ValueRecord.parseSchemaId(value);
    VALUE object = getFromStorage(
        value,
        (value == null ? 0 : value.length),
        0,
        adapter,
        store::get,
        store.getStoreVersionName(),
        partition,
        null,
        reusedValue,
        reusedDecoder,
        -1,
        storeDeserializerCache,
        compressor,
        manifestContainer);
    if (writerSchemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
      writerSchemaId = Objects.requireNonNull(manifestContainer, "The ChunkedValueManifestContainer cannot be null.")
          .getManifest()
          .getSchemaId();
    }
    return new ByteBufferValueRecord<>(object, writerSchemaId);
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
   * @see SingleGetChunkingAdapter#get(AbstractStorageEngine, int, byte[], boolean, ReadResponseStats)
   * @see BatchGetChunkingAdapter#get(AbstractStorageEngine, int, ByteBuffer, boolean, ReadResponseStats)
   */
  private static <VALUE, CHUNKS_CONTAINER> VALUE getFromStorage(
      byte[] value,
      int valueLength,
      long databaseLookupStartTimeInNS,
      ChunkingAdapter<CHUNKS_CONTAINER, VALUE> adapter,
      StorageGetFunction storageGetFunction,
      String storeVersionName,
      int partition,
      ReadResponseStats response,
      VALUE reusedValue,
      BinaryDecoder reusedDecoder,
      int readerSchemaId,
      StoreDeserializerCache<VALUE> storeDeserializerCache,
      VeniceCompressor compressor,
      ChunkedValueManifestContainer manifestContainer) {

    if (value == null) {
      return null;
    }
    int writerSchemaId = ValueRecord.parseSchemaId(value);

    if (writerSchemaId > 0) {
      // User-defined schema, thus not a chunked value. Early termination.

      if (response != null) {
        response.addDatabaseLookupLatency(LatencyUtils.getElapsedTimeFromNSToMS(databaseLookupStartTimeInNS));
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

    byte[] valueChunk;
    for (int chunkIndex = 0; chunkIndex < chunkedValueManifest.keysWithChunkIdSuffix.size(); chunkIndex++) {
      // N.B.: This is done sequentially. Originally, each chunk was fetched concurrently in the same executor
      // as the main queries, but this might cause deadlocks, so we are now doing it sequentially. If we want to
      // optimize large value retrieval in the future, it's unclear whether the concurrent retrieval approach
      // is optimal (as opposed to streaming the response out incrementally, for example). Since this is a
      // premature optimization, we are not addressing it right now.
      valueChunk = storageGetFunction.apply(partition, chunkedValueManifest.keysWithChunkIdSuffix.get(chunkIndex));

      if (valueChunk == null) {
        throw new VeniceException(
            "Chunk not found in " + getExceptionMessageDetails(storeVersionName, partition, chunkIndex));
      } else if (ValueRecord.parseSchemaId(valueChunk) != AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()) {
        throw new VeniceException(
            "Did not get the chunk schema ID while attempting to retrieve a chunk! " + "Instead, got schema ID: "
                + ValueRecord.parseSchemaId(valueChunk) + " from "
                + getExceptionMessageDetails(storeVersionName, partition, chunkIndex));
      }

      actualSize += valueChunk.length - ValueRecord.SCHEMA_HEADER_LENGTH;
      adapter.addChunkIntoContainer(assembledValueContainer, chunkIndex, valueChunk);
    }

    // Sanity check based on size...
    if (actualSize != chunkedValueManifest.size) {
      throw new VeniceException(
          "The fully assembled large value does not have the expected size! " + "actualSize: " + actualSize
              + ", chunkedValueManifest.size: " + chunkedValueManifest.size + ", "
              + getExceptionMessageDetails(storeVersionName, partition, null));
    }

    if (response != null) {
      response.addDatabaseLookupLatency(LatencyUtils.getElapsedTimeFromNSToMS(databaseLookupStartTimeInNS));
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

  private static String getExceptionMessageDetails(String storeVersionName, int partition, Integer chunkIndex) {
    String message = "store-version: " + storeVersionName + ", partition: " + partition;
    if (chunkIndex != null) {
      message += ", chunk index: " + chunkIndex;
    }
    message += ".";
    return message;
  }
}
