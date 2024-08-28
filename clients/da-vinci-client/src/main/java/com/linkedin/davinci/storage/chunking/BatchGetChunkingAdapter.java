package com.linkedin.davinci.storage.chunking;

import com.linkedin.davinci.listener.response.ReadResponseStats;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import java.nio.ByteBuffer;


/**
 * Batch get chunking adapter
 */
public class BatchGetChunkingAdapter implements ChunkingAdapter<ByteBuffer, MultiGetResponseRecordV1> {
  private static final BatchGetChunkingAdapter BATCH_GET_CHUNKING_ADAPTER = new BatchGetChunkingAdapter();

  /** Singleton */
  private BatchGetChunkingAdapter() {
  }

  @Override
  public void addChunkIntoContainer(ByteBuffer byteBuffer, int chunkIndex, byte[] valueChunk) {
    byteBuffer.put(valueChunk, ValueRecord.SCHEMA_HEADER_LENGTH, valueChunk.length - ValueRecord.SCHEMA_HEADER_LENGTH);
  }

  @Override
  public ByteBuffer constructChunksContainer(ChunkedValueManifest chunkedValueManifest) {
    return ByteBuffer.allocate(chunkedValueManifest.size);
  }

  @Override
  public MultiGetResponseRecordV1 constructValue(int schemaId, ByteBuffer byteBuffer) {
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
  }

  @Override
  public MultiGetResponseRecordV1 constructValue(int schemaId, byte[] fullBytes) {
    MultiGetResponseRecordV1 record = new MultiGetResponseRecordV1();
    /** N.B.: Does not need any repositioning, as opposed to {@link containerToMultiGetResponseRecord} */
    record.value = ValueRecord.parseDataAsNIOByteBuffer(fullBytes);
    record.schemaId = schemaId;
    return record;
  }

  public static MultiGetResponseRecordV1 get(
      AbstractStorageEngine store,
      int partition,
      ByteBuffer key,
      boolean isChunked,
      ReadResponseStats response) {
    if (isChunked) {
      key = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key);
    }
    return ChunkingUtils.getFromStorage(BATCH_GET_CHUNKING_ADAPTER, store, partition, key, response);
  }
}
