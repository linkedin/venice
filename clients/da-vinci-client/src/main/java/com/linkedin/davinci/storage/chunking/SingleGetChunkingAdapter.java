package com.linkedin.davinci.storage.chunking;

import com.linkedin.davinci.listener.response.ReadResponseStats;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;


/**
 * Single get chunking adapter
 */
public class SingleGetChunkingAdapter implements ChunkingAdapter<CompositeByteBuf, ValueRecord> {
  private static final SingleGetChunkingAdapter SINGLE_GET_CHUNKING_ADAPTER = new SingleGetChunkingAdapter();

  /** Singleton */
  private SingleGetChunkingAdapter() {
  }

  @Override
  public void addChunkIntoContainer(CompositeByteBuf byteBufs, int chunkIndex, byte[] valueChunk) {
    byteBufs.addComponent(true, chunkIndex, ValueRecord.parseDataAsByteBuf(valueChunk));
  }

  @Override
  public CompositeByteBuf constructChunksContainer(ChunkedValueManifest chunkedValueManifest) {
    return Unpooled.compositeBuffer(chunkedValueManifest.keysWithChunkIdSuffix.size());
  }

  @Override
  public ValueRecord constructValue(int schemaId, CompositeByteBuf byteBufs) {
    return ValueRecord.create(schemaId, byteBufs);
  }

  @Override
  public ValueRecord constructValue(int schemaId, byte[] fullBytes) {
    return ValueRecord.parseAndCreate(fullBytes);
  }

  public static ValueRecord get(
      AbstractStorageEngine store,
      int partition,
      byte[] key,
      boolean isChunked,
      ReadResponseStats response) {
    ByteBuffer keyBuffer = isChunked
        ? ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKeyAsByteBuffer(key)
        : ByteBuffer.wrap(key);
    return ChunkingUtils.getFromStorage(SINGLE_GET_CHUNKING_ADAPTER, store, partition, keyBuffer, response);
  }

  public static ValueRecord getReplicationMetadata(
      AbstractStorageEngine store,
      int partition,
      byte[] key,
      boolean isChunked,
      ChunkedValueManifestContainer manifestContainer) {
    ByteBuffer keyBuffer = isChunked
        ? ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKeyAsByteBuffer(key)
        : ByteBuffer.wrap(key);
    return ChunkingUtils
        .getReplicationMetadataFromStorage(SINGLE_GET_CHUNKING_ADAPTER, store, partition, keyBuffer, manifestContainer);
  }
}
