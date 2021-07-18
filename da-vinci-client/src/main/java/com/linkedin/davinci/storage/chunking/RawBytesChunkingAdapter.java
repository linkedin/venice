package com.linkedin.davinci.storage.chunking;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.serializer.IdentityRecordDeserializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import java.nio.ByteBuffer;

/**
 * A ChunkingAdapter to be used when we want to read the value from storage engine as raw bytes. The
 * {@link AbstractAvroChunkingAdapter} merges the separate chunks and returns a complete value.
 */
public class RawBytesChunkingAdapter extends AbstractAvroChunkingAdapter<ByteBuffer> {
  public static final RawBytesChunkingAdapter INSTANCE = new RawBytesChunkingAdapter();

  /** Singleton */
  protected RawBytesChunkingAdapter() {
    super();
  }

  @Override
  protected RecordDeserializer<ByteBuffer> getDeserializer(String storeName, int schemaId, ReadOnlySchemaRepository schemaRepo, boolean fastAvroEnabled) {
    return IdentityRecordDeserializer.getInstance();
  }
}
