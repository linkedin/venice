package com.linkedin.venice.storage.chunking;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.response.ReadResponse;
import com.linkedin.venice.listener.response.StorageResponseObject;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.record.ValueRecord;
import com.linkedin.venice.writer.VeniceWriter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;

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
  private final static ChunkedValueManifestSerializer chunkedValueManifestSerializer = new ChunkedValueManifestSerializer(false);
  private final static KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();

  /**
   * Handles the retrieval of a {@link ValueRecord}, handling everything specific to this
   * code path, including chunking the key and dealing with {@link ByteBuf} or {@link CompositeByteBuf}.
   *
   * This is used by the single get code path.
   */
  public static ValueRecord getValueRecord(AbstractStorageEngine store, int partition, byte[] key, boolean isChunked, StorageResponseObject response) {
    ByteBuffer keyBuffer = null;
    if (isChunked) {
      keyBuffer = ByteBuffer.wrap(keyWithChunkingSuffixSerializer.serializeNonChunkedKey(key));
    } else {
      keyBuffer = ByteBuffer.wrap(key);
    }
    return getFromStorage(
        store,
        partition,
        keyBuffer,
        response,
        fullBytesToValueRecord,
        constructCompositeByteBuf,
        addChunkIntoCompositeByteBuf,
        getSizeOfCompositeByteBuf,
        containerToValueRecord);
  }

  /**
   * Handles the retrieval of a {@link MultiGetResponseRecordV1}, handling everything specific to this
   * code path, including chunking the key and dealing with {@link ByteBuffer}.
   *
   * This is used by the batch get and compute code paths.
   */
  public static MultiGetResponseRecordV1 getMultiGetResponseRecordV1(
      AbstractStorageEngine store,
      int partition,
      ByteBuffer key,
      boolean isChunked,
      ReadResponse response) {
    if (isChunked) {
      key = ByteBuffer.wrap(keyWithChunkingSuffixSerializer.serializeNonChunkedKey(key));
    }
    MultiGetResponseRecordV1 record = getFromStorage(
        store,
        partition,
        key,
        response,
        fullBytesToMultiGetResponseRecordV1,
        constructNioByteBuffer,
        addChunkIntoNioByteBuffer,
        getSizeOfNioByteBuffer,
        containerToMultiGetResponseRecord);

    return record;
  }

  // Single get chunking functions

  private static final FullBytesToValue<ValueRecord> fullBytesToValueRecord = (schemaId, fullBytes) -> ValueRecord.parseAndCreate(fullBytes);
  private static final ConstructAssembledValueContainer<CompositeByteBuf> constructCompositeByteBuf = chunkedValueManifest ->
      Unpooled.compositeBuffer(chunkedValueManifest.keysWithChunkIdSuffix.size());
  private static final AddChunkIntoContainer<CompositeByteBuf> addChunkIntoCompositeByteBuf = (o, chunkIndex, valueChunk) ->
      o.addComponent(true, chunkIndex, ValueRecord.parseDataAsByteBuf(valueChunk));
  private static final GetSizeOfContainer<CompositeByteBuf> getSizeOfCompositeByteBuf = byteBuf -> byteBuf.readableBytes();
  private static final ContainerToValue<CompositeByteBuf, ValueRecord> containerToValueRecord = (schemaId, byteBuf) -> ValueRecord.create(schemaId, byteBuf);

  // Batch get and compute functions

  private static final FullBytesToValue<MultiGetResponseRecordV1> fullBytesToMultiGetResponseRecordV1 = (schemaId, fullBytes) -> {
    MultiGetResponseRecordV1 record = new MultiGetResponseRecordV1();
    /** N.B.: Does not need any repositioning, as opposed to {@link containerToMultiGetResponseRecord} */
    record.value = ValueRecord.parseDataAsNIOByteBuffer(fullBytes);
    record.schemaId = schemaId;
    return record;
  };
  private static final ConstructAssembledValueContainer<ByteBuffer> constructNioByteBuffer = chunkedValueManifest -> ByteBuffer.allocate(chunkedValueManifest.size);
  private static final AddChunkIntoContainer<ByteBuffer> addChunkIntoNioByteBuffer = (byteBuffer, chunkIndex, valueChunk) ->
      byteBuffer.put(valueChunk, ValueRecord.SCHEMA_HEADER_LENGTH, valueChunk.length - ValueRecord.SCHEMA_HEADER_LENGTH);
  private static final GetSizeOfContainer<ByteBuffer> getSizeOfNioByteBuffer = byteBuffer -> byteBuffer.position();
  private static final ContainerToValue<ByteBuffer, MultiGetResponseRecordV1> containerToMultiGetResponseRecord = (schemaId, byteBuffer) -> {
    MultiGetResponseRecordV1 record = new MultiGetResponseRecordV1();
    /**
     * For re-assembled large values, it is necessary to reposition the {@link ByteBuffer} back to the
     * beginning of its content, otherwise the Avro encoder will skip this content.
     *
     * Note that this only occurs for a ByteBuffer we've been writing into gradually (i.e.: during chunk
     * re-assembly) but does not occur for a ByteBuffer that has been created by wrapping a single byte
     * array (i.e.: as is the case for small values, in {@link fullBytesToMultiGetResponseRecordV1}).
     * Doing this re-positioning for small values would cause another type of problem, because for these
     * (wrapping) ByteBuffer instances, the position needs to remain set to the starting offset (within
     * the backing array) which was originally specified at construction time...
     */
    byteBuffer.position(0);
    record.value = byteBuffer;
    record.schemaId = schemaId;
    return record;
  };

  // The code below is general-purpose storage engine handling and chunking re-assembly. The specifics
  // of single gets vs batch gets are abstracted away by a handful of functional interfaces.

  /**
   * Fetches the value associated with the given key, and potentially re-assembles it, if it is
   * a chunked value. This function should not be called directly, from the query code, as it expects
   * the key to be properly formatted already. Use of one these simpler functions instead:
   *
   * @see #getValueRecord(AbstractStorageEngine, int, byte[], boolean, StorageResponseObject)
   * @see #getMultiGetResponseRecordV1(AbstractStorageEngine, int, ByteBuffer, boolean, ReadResponse)
   *
   * This code makes use of several functional interfaces in order to abstract away the different needs
   * of the single get and batch get query paths.
   */
  private static <VALUE, ASSEMBLED_VALUE_CONTAINER> VALUE getFromStorage(
      AbstractStorageEngine store,
      int partition,
      ByteBuffer keyBuffer,
      ReadResponse response,
      FullBytesToValue<VALUE> fullBytesToValueFunction,
      ConstructAssembledValueContainer<ASSEMBLED_VALUE_CONTAINER> constructAssembledValueContainer,
      AddChunkIntoContainer addChunkIntoContainerFunction,
      GetSizeOfContainer getSizeFromContainerFunction,
      ContainerToValue<ASSEMBLED_VALUE_CONTAINER, VALUE> containerToValueFunction) {

    byte[] value = store.get(partition, keyBuffer);
    if (null == value) {
      return null;
    }
    int schemaId = ValueRecord.parseSchemaId(value);

    if (schemaId > 0) {
      // User-defined schema, thus not a chunked value. Early termination.
      return fullBytesToValueFunction.construct(schemaId, value);
    } else if (schemaId != AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
      throw new VeniceException("Found a record with invalid schema ID: " + schemaId);
    }

    // End of initial sanity checks. We have chunked value, so we need to fetch all chunks

    ChunkedValueManifest chunkedValueManifest = chunkedValueManifestSerializer.deserialize(value, schemaId);
    ASSEMBLED_VALUE_CONTAINER assembledValueContainer = constructAssembledValueContainer.construct(chunkedValueManifest);

    for (int chunkIndex = 0; chunkIndex < chunkedValueManifest.keysWithChunkIdSuffix.size(); chunkIndex++) {
      // N.B.: This is done sequentially. Originally, each chunk was fetched concurrently in the same executor
      // as the main queries, but this might cause deadlocks, so we are now doing it sequentially. If we want to
      // optimize large value retrieval in the future, it's unclear whether the concurrent retrieval approach
      // is optimal (as opposed to streaming the response out incrementally, for example). Since this is a
      // premature optimization, we are not addressing it right now.
      byte[] valueChunk = store.get(partition, chunkedValueManifest.keysWithChunkIdSuffix.get(chunkIndex).array());

      if (null == valueChunk) {
        throw new VeniceException(
            "Chunk not found in " + getExceptionMessageDetails(store, partition, chunkIndex));
      } else if (ValueRecord.parseSchemaId(valueChunk) != AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()) {
        throw new VeniceException(
            "Did not get the chunk schema ID while attempting to retrieve a chunk! " + "Instead, got schema ID: " + ValueRecord.parseSchemaId(valueChunk) + " from "
                + getExceptionMessageDetails(store, partition, chunkIndex));
      }

      addChunkIntoContainerFunction.add(assembledValueContainer, chunkIndex, valueChunk);
    }

    // Sanity check based on size...
    int actualSize = getSizeFromContainerFunction.sizeOf(assembledValueContainer);
    if (actualSize != chunkedValueManifest.size) {
      throw new VeniceException(
          "The fully assembled large value does not have the expected size! " + "actualSize: " + actualSize + ", chunkedValueManifest.size: " + chunkedValueManifest.size
              + ", " + getExceptionMessageDetails(store, partition, null));
    }

    response.incrementMultiChunkLargeValueCount();

    return containerToValueFunction.construct(chunkedValueManifest.schemaId, assembledValueContainer);
  }

  private static String getExceptionMessageDetails(AbstractStorageEngine store, int partition, Integer chunkIndex) {
    String message = "store: " + store.getName() + ", partition: " + partition;
    if (chunkIndex != null) {
      message += ", chunk index: " + chunkIndex;
    }
    message += ".";
    return message;
  }
}
