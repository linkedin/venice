package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.davinci.replication.merge.MergeConflictResult;
import com.linkedin.davinci.storage.chunking.ChunkedValueManifestContainer;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;


/**
 * This wrapper is used to keep the context after handling Active/Active messages.
 */
public class MergeConflictResultWrapper {
  private final MergeConflictResult mergeConflictResult;
  private final Lazy<ByteBufferValueRecord<ByteBuffer>> oldValueProvider;
  private final Lazy<ByteBuffer> oldValueByteBufferProvider;
  private final RmdWithValueSchemaId oldRmdWithValueSchemaId;
  private final ChunkedValueManifestContainer oldValueManifestContainer;

  // Serialized and potentially compressed updated value bytes
  private final ByteBuffer updatedValueBytes;
  private final ByteBuffer updatedRmdBytes;

  /**
   * Best-effort deserialized value provider that provides the updated value for PUT/UPDATE and the old value for
   * DELETE.
   */
  private final Lazy<GenericRecord> valueProvider;

  public MergeConflictResultWrapper(
      MergeConflictResult mergeConflictResult,
      Lazy<ByteBufferValueRecord<ByteBuffer>> oldValueProvider,
      Lazy<ByteBuffer> oldValueByteBufferProvider,
      RmdWithValueSchemaId oldRmdWithValueSchemaId,
      ChunkedValueManifestContainer oldValueManifestContainer,
      ByteBuffer updatedValueBytes,
      ByteBuffer updatedRmdBytes,
      Function<Integer, RecordDeserializer<GenericRecord>> deserializerProvider) {
    this.mergeConflictResult = mergeConflictResult;
    this.oldValueProvider = oldValueProvider;
    this.oldValueByteBufferProvider = oldValueByteBufferProvider;
    this.oldRmdWithValueSchemaId = oldRmdWithValueSchemaId;
    this.oldValueManifestContainer = oldValueManifestContainer;
    this.updatedValueBytes = updatedValueBytes;
    this.updatedRmdBytes = updatedRmdBytes;
    if (updatedValueBytes == null) {
      // this is a DELETE
      ByteBufferValueRecord<ByteBuffer> oldValue = oldValueProvider.get();
      if (oldValue == null || oldValue.value() == null) {
        this.valueProvider = Lazy.of(() -> null);
      } else {
        this.valueProvider =
            Lazy.of(() -> deserializerProvider.apply(oldValue.writerSchemaId()).deserialize(oldValue.value()));
      }
    } else {
      // this is a PUT or UPDATE
      if (mergeConflictResult.getDeserializedValue().isPresent()) {
        this.valueProvider = Lazy.of(() -> mergeConflictResult.getDeserializedValue().get());
      } else {
        // Use mergeConflictResult.getNewValue() here since updatedValueBytes could be compressed.
        this.valueProvider = Lazy.of(
            () -> deserializerProvider.apply(mergeConflictResult.getValueSchemaId())
                .deserialize(mergeConflictResult.getNewValue()));
      }
    }
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

  /**
   * Return a best-effort value provider with the following behaviors:
   *   1. returns the new value provider for PUT and UPDATE.
   *   2. returns the old value for DELETE (null for non-existent key).
   *   3. returns null if the value is not available.
   */
  public Lazy<GenericRecord> getValueProvider() {
    return valueProvider;
  }
}
