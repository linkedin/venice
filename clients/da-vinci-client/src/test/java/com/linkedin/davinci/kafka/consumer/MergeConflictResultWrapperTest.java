package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.davinci.replication.merge.MergeConflictResult;
import com.linkedin.davinci.storage.chunking.ChunkedValueManifestContainer;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MergeConflictResultWrapperTest {
  @Test
  public void testValueProvider() {
    MergeConflictResult mergeConflictResult = mock(MergeConflictResult.class);
    RecordDeserializer<GenericRecord> deserializer = mock(RecordDeserializer.class);
    GenericRecord mockDeleteOldRecord = mock(GenericRecord.class);
    GenericRecord mockNewValueRecord = mock(GenericRecord.class);
    ByteBuffer mockDeleteOldValueBytes = mock(ByteBuffer.class);
    ByteBuffer mockNewValueBytes = mock(ByteBuffer.class);
    doReturn(mockDeleteOldRecord).when(deserializer).deserialize(mockDeleteOldValueBytes);
    doReturn(mockNewValueRecord).when(deserializer).deserialize(mockNewValueBytes);
    RmdWithValueSchemaId rmdWithValueSchemaId = mock(RmdWithValueSchemaId.class);
    ChunkedValueManifestContainer chunkedValueManifestContainer = mock(ChunkedValueManifestContainer.class);
    ByteBuffer mockUpdatedRmdBytes = mock(ByteBuffer.class);
    Function<Integer, RecordDeserializer<GenericRecord>> deserProvider = (schemaId) -> deserializer;
    // DELETE
    MergeConflictResultWrapper nonExistingKeyDeleteWrapper = new MergeConflictResultWrapper(
        mergeConflictResult,
        Lazy.of(() -> null),
        Lazy.of(() -> null),
        rmdWithValueSchemaId,
        chunkedValueManifestContainer,
        null,
        mockUpdatedRmdBytes,
        deserProvider);
    Assert.assertNull(nonExistingKeyDeleteWrapper.getValueProvider().get());
    ByteBufferValueRecord<ByteBuffer> mockDeleteByteBufferValueRecord = mock(ByteBufferValueRecord.class);
    doReturn(mockDeleteOldValueBytes).when(mockDeleteByteBufferValueRecord).value();
    MergeConflictResultWrapper existingKeyDeleteWrapper = new MergeConflictResultWrapper(
        mergeConflictResult,
        Lazy.of(() -> mockDeleteByteBufferValueRecord),
        Lazy.of(() -> mockDeleteOldValueBytes),
        rmdWithValueSchemaId,
        chunkedValueManifestContainer,
        null,
        mockUpdatedRmdBytes,
        deserProvider);
    Assert.assertEquals(existingKeyDeleteWrapper.getValueProvider().get(), mockDeleteOldRecord);
    // PUT/UPDATE
    ByteBuffer mockUpdatedValueBytes = mock(ByteBuffer.class);
    doReturn(Optional.of(mockNewValueRecord)).when(mergeConflictResult).getDeserializedValue();
    MergeConflictResultWrapper updateCachedWrapper = new MergeConflictResultWrapper(
        mergeConflictResult,
        Lazy.of(() -> null),
        Lazy.of(() -> null),
        rmdWithValueSchemaId,
        chunkedValueManifestContainer,
        mockUpdatedValueBytes,
        mockUpdatedRmdBytes,
        deserProvider);
    Assert.assertEquals(updateCachedWrapper.getValueProvider().get(), mockNewValueRecord);
    doReturn(Optional.empty()).when(mergeConflictResult).getDeserializedValue();
    doReturn(mockNewValueBytes).when(mergeConflictResult).getNewValue();
    MergeConflictResultWrapper updateNotCachedWrapper = new MergeConflictResultWrapper(
        mergeConflictResult,
        Lazy.of(() -> null),
        Lazy.of(() -> null),
        rmdWithValueSchemaId,
        chunkedValueManifestContainer,
        mockUpdatedValueBytes,
        mockUpdatedRmdBytes,
        deserProvider);
    Assert.assertEquals(updateNotCachedWrapper.getValueProvider().get(), mockNewValueRecord);
  }
}
