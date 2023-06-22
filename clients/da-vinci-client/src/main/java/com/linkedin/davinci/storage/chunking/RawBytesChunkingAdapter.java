package com.linkedin.davinci.storage.chunking;

import java.nio.ByteBuffer;


/**
 * A ChunkingAdapter to be used when we want to read the value from storage engine as raw bytes. The
 * {@link AbstractAvroChunkingAdapter} merges the separate chunks and returns a complete value.
 *
 * @see {@link RawBytesStoreDeserializerCache}
 */
public class RawBytesChunkingAdapter extends AbstractAvroChunkingAdapter<ByteBuffer> {
  public static final RawBytesChunkingAdapter INSTANCE = new RawBytesChunkingAdapter();

  /** Singleton */
  protected RawBytesChunkingAdapter() {
    super();
  }
}
