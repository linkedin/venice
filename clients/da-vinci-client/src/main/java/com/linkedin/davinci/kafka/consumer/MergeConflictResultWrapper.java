package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.davinci.replication.merge.MergeConflictResult;
import com.linkedin.davinci.storage.chunking.ChunkedValueManifestContainer;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;


/**
 * This wrapper is used to keep the context after handling Active/Active messages.
 */
public class MergeConflictResultWrapper {
  private final MergeConflictResult mergeConflictResult;
  private final Lazy<ByteBufferValueRecord<ByteBuffer>> oldValueProvider;
  private final Lazy<ByteBuffer> oldValueByteBufferProvider;
  private final RmdWithValueSchemaId oldRmdWithValueSchemaId;
  private final ChunkedValueManifestContainer oldValueManifestContainer;
  private final ByteBuffer updatedValueBytes;
  private final ByteBuffer updatedRmdBytes;

  public MergeConflictResultWrapper(
      MergeConflictResult mergeConflictResult,
      Lazy<ByteBufferValueRecord<ByteBuffer>> oldValueProvider,
      Lazy<ByteBuffer> oldValueByteBufferProvider,
      RmdWithValueSchemaId oldRmdWithValueSchemaId,
      ChunkedValueManifestContainer oldValueManifestContainer,
      ByteBuffer updatedValueBytes,
      ByteBuffer updatedRmdBytes) {
    this.mergeConflictResult = mergeConflictResult;
    this.oldValueProvider = oldValueProvider;
    this.oldValueByteBufferProvider = oldValueByteBufferProvider;
    this.oldRmdWithValueSchemaId = oldRmdWithValueSchemaId;
    this.oldValueManifestContainer = oldValueManifestContainer;
    this.updatedValueBytes = updatedValueBytes;
    this.updatedRmdBytes = updatedRmdBytes;
  }

  public MergeConflictResult getMergeConflictResult() {
    return mergeConflictResult;
  }

  public Lazy<ByteBuffer> getOldValueByteBufferProvider() {
    return oldValueByteBufferProvider;
  }

  public RmdWithValueSchemaId getOldRmdWithValueSchemaId() {
    return oldRmdWithValueSchemaId;
  }

  public ChunkedValueManifestContainer getOldValueManifestContainer() {
    return oldValueManifestContainer;
  }

  public Lazy<ByteBufferValueRecord<ByteBuffer>> getOldValueProvider() {
    return oldValueProvider;
  }

  public ByteBuffer getUpdatedValueBytes() {
    return updatedValueBytes;
  }

  public ByteBuffer getUpdatedRmdBytes() {
    return updatedRmdBytes;
  }
}
