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
   * Best-effort deserialized value provider that provides the updated value for PUT/UPDATE
   */
  private final Lazy<GenericRecord> valueProvider;
  /**
   * Best-effort deserialized value provider that provides the updated value for PUT/UPDATE/DELETE. Returns null if the
   * key/value didn't exist
   */
  private final Lazy<GenericRecord> deserializedOldValueProvider;

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

    // We will always configure the deserializedOldValueProvider. Theoretically we could cache the deserialized old
    // value in the UPDATE branch, but it will require a deep copy since the record is used for in-place update(s). To
    // reduce complexity we are just going to deserialize the old bytes.
    this.deserializedOldValueProvider = Lazy.of(() -> {
      ByteBufferValueRecord<ByteBuffer> oldValue = oldValueProvider.get();
      if (oldValue == null || oldValue.value() == null) {
        return null;
      } else {
        return deserializerProvider.apply(oldValue.writerSchemaId()).deserialize(oldValue.value());
      }
    });
    if (updatedValueBytes == null) {
      // this is a DELETE
      this.valueProvider = Lazy.of(() -> null);
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
   *   3. returns null if the value is not available.
   */
  public Lazy<GenericRecord> getValueProvider() {
    return valueProvider;
  }

  public Lazy<GenericRecord> getDeserializedOldValueProvider() {
    return deserializedOldValueProvider;
  }
}
