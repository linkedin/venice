package com.linkedin.venice.writer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.schema.writecompute.WriteComputeHandler;
import com.linkedin.venice.schema.writecompute.WriteComputeHandlerV1;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.StringSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BatchingVeniceWriterTest {
  private static final Schema VALUE_SCHEMA = AvroCompatibilityHelper.parse(TestUtils.loadFileAsString("PersonV1.avsc"));
  private static final Schema UPDATE_SCHEMA =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA);
  private static final RecordSerializer<GenericRecord> valueSerializer =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(VALUE_SCHEMA);
  private static final RecordDeserializer<GenericRecord> valueDeserializer =
      FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(VALUE_SCHEMA, VALUE_SCHEMA);
  private static final RecordSerializer<GenericRecord> updateSerializer =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(UPDATE_SCHEMA);
  private static final RecordDeserializer<GenericRecord> updateDeserializer =
      FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(UPDATE_SCHEMA, UPDATE_SCHEMA);
  private static final WriteComputeHandler updateHandler = new WriteComputeHandlerV1();

  @Test
  public void testSendRecord() {
    BatchingVeniceWriter<byte[], byte[], byte[]> writer = mock(BatchingVeniceWriter.class);
    VeniceWriter<byte[], byte[], byte[]> internalWriter = mock(VeniceWriter.class);
    doReturn(internalWriter).when(writer).getVeniceWriter();
    doCallRealMethod().when(writer).sendRecord(any());
    byte[] keyBytes = "abc".getBytes();
    byte[] valueBytes = "def".getBytes();
    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    CompletableFutureCallback completableFutureCallback = new CompletableFutureCallback(completableFuture);
    int valueSchemaId = 100;
    int protocolId = 200;
    long logicalTimestamp = 10000L;

    ProducerBufferRecord putRecord = new ProducerBufferRecord(
        MessageType.PUT,
        keyBytes,
        valueBytes,
        null,
        valueSchemaId,
        protocolId,
        completableFutureCallback,
        logicalTimestamp);
    writer.sendRecord(putRecord);
    verify(internalWriter, times(1)).put(eq(keyBytes), eq(valueBytes), eq(valueSchemaId), eq(logicalTimestamp), any());

    ProducerBufferRecord deleteRecord = new ProducerBufferRecord(
        MessageType.DELETE,
        keyBytes,
        null,
        null,
        valueSchemaId,
        protocolId,
        completableFutureCallback,
        logicalTimestamp);
    writer.sendRecord(deleteRecord);
    verify(internalWriter, times(1)).delete(eq(keyBytes), eq(logicalTimestamp), any());

    ProducerBufferRecord updateRecord = new ProducerBufferRecord(
        MessageType.UPDATE,
        keyBytes,
        null,
        valueBytes,
        valueSchemaId,
        protocolId,
        completableFutureCallback,
        logicalTimestamp);
    writer.sendRecord(updateRecord);
    verify(internalWriter, times(1))
        .update(eq(keyBytes), eq(valueBytes), eq(valueSchemaId), eq(protocolId), any(), eq(logicalTimestamp));
  }

  @Test
  public void testUpdateAndPut() {
    List<ProducerBufferRecord> bufferRecordList = new ArrayList<>();
    Map<ByteBuffer, ProducerBufferRecord> bufferRecordIndex = new VeniceConcurrentHashMap<>();
    List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
    List<CompletableFutureCallback> completableFutureCallbackList = new ArrayList<>();
    int numberOfOperations = 2;
    BatchingVeniceWriter<String, GenericRecord, GenericRecord> writer = prepareMockSetup(
        numberOfOperations,
        completableFutureList,
        completableFutureCallbackList,
        bufferRecordIndex,
        bufferRecordList);

    String key = "a";
    byte[] serializedKey = key.getBytes();
    GenericRecord updateRecord = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", "K").build();
    byte[] updateBytes = updateSerializer.serialize(updateRecord);

    GenericRecord valueRecord = new GenericData.Record(VALUE_SCHEMA);
    valueRecord.put("name", "J");
    valueRecord.put("age", 0);
    valueRecord.put("intArray", Collections.emptyList());
    valueRecord.put("recordArray", Collections.emptyList());
    valueRecord.put("stringMap", Collections.emptyMap());
    valueRecord.put("recordMap", Collections.emptyMap());
    byte[] valueBytes = valueSerializer.serialize(valueRecord);
    writer.update(key, updateRecord, 1, 1, completableFutureCallbackList.get(0));
    writer.put(key, valueRecord, 1, completableFutureCallbackList.get(1));

    Assert.assertEquals(bufferRecordList.size(), numberOfOperations);
    Assert.assertFalse(bufferRecordIndex.isEmpty());
    Assert.assertEquals(bufferRecordIndex.get(ByteBuffer.wrap(serializedKey)).getSerializedValue(), valueBytes);

    Assert.assertEquals(bufferRecordList.get(0).getSerializedUpdate(), updateBytes);
    Assert.assertTrue(bufferRecordList.get(0).shouldSkipProduce());
    Assert.assertEquals(bufferRecordList.get(0).getTimestamp(), VeniceWriter.APP_DEFAULT_LOGICAL_TS);

    Assert.assertEquals(bufferRecordList.get(1).getSerializedValue(), valueBytes);
    Assert.assertFalse(bufferRecordList.get(1).shouldSkipProduce());
    Assert.assertEquals(bufferRecordList.get(1).getTimestamp(), VeniceWriter.APP_DEFAULT_LOGICAL_TS);

    // Perform produce operation
    writer.checkAndMaybeProduceBatchRecord();
    verify(writer, times(1)).sendRecord(any());
    Assert.assertTrue(bufferRecordList.isEmpty());
    Assert.assertTrue(bufferRecordIndex.isEmpty());

    // Capture writer behavior.
    ArgumentCaptor<PubSubProducerCallback> putCallbackCaptor = ArgumentCaptor.forClass(PubSubProducerCallback.class);
    verify(writer.getVeniceWriter(), times(1)).put(any(), any(), eq(1), anyLong(), putCallbackCaptor.capture());

    // Verify final callback is ChainedPubSubCallback
    PubSubProducerCallback putCallback = putCallbackCaptor.getValue();
    Assert.assertTrue(putCallback instanceof ChainedPubSubCallback);
    Assert.assertEquals(
        ((CompletableFutureCallback) ((ChainedPubSubCallback) putCallback).getDependentCallbackList().get(0))
            .getCompletableFuture(),
        completableFutureList.get(0));
    Assert.assertEquals(
        ((CompletableFutureCallback) ((ChainedPubSubCallback) putCallback).getCallback()).getCompletableFuture(),
        completableFutureList.get(1));

    // Verify ChainedPubSubCallback behavior
    putCallback.onCompletion(null, null);
    Assert.assertTrue(completableFutureList.get(0).isDone());
    Assert.assertTrue(completableFutureList.get(1).isDone());
  }

  @Test
  public void testPutWithLogicalTimestamp() {
    List<ProducerBufferRecord> bufferRecordList = new ArrayList<>();
    Map<ByteBuffer, ProducerBufferRecord> bufferRecordIndex = new VeniceConcurrentHashMap<>();
    List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
    List<CompletableFutureCallback> completableFutureCallbackList = new ArrayList<>();
    int numberOfOperations = 2;
    BatchingVeniceWriter<String, GenericRecord, GenericRecord> writer = prepareMockSetup(
        numberOfOperations,
        completableFutureList,
        completableFutureCallbackList,
        bufferRecordIndex,
        bufferRecordList);

    String key = "a";
    byte[] serializedKey = key.getBytes();

    GenericRecord valueRecord = new GenericData.Record(VALUE_SCHEMA);
    valueRecord.put("name", "J");
    valueRecord.put("age", 0);
    valueRecord.put("intArray", Collections.emptyList());
    valueRecord.put("recordArray", Collections.emptyList());
    valueRecord.put("stringMap", Collections.emptyMap());
    valueRecord.put("recordMap", Collections.emptyMap());
    byte[] valueBytes = valueSerializer.serialize(valueRecord);
    writer.put(key, valueRecord, 1, 1000L, completableFutureCallbackList.get(0));
    writer.put(key, valueRecord, 1, completableFutureCallbackList.get(1));

    Assert.assertEquals(bufferRecordList.size(), numberOfOperations);
    Assert.assertFalse(bufferRecordIndex.isEmpty());
    Assert.assertEquals(bufferRecordIndex.get(ByteBuffer.wrap(serializedKey)).getSerializedValue(), valueBytes);

    Assert.assertEquals(bufferRecordList.get(0).getSerializedValue(), valueBytes);
    Assert.assertFalse(bufferRecordList.get(0).shouldSkipProduce());
    Assert.assertEquals(bufferRecordList.get(0).getTimestamp(), 1000L);

    Assert.assertEquals(bufferRecordList.get(1).getSerializedValue(), valueBytes);
    Assert.assertFalse(bufferRecordList.get(1).shouldSkipProduce());
    Assert.assertEquals(bufferRecordList.get(1).getTimestamp(), VeniceWriter.APP_DEFAULT_LOGICAL_TS);

    // Perform produce operation
    writer.checkAndMaybeProduceBatchRecord();
    verify(writer, times(2)).sendRecord(any());
    Assert.assertTrue(bufferRecordList.isEmpty());
    Assert.assertTrue(bufferRecordIndex.isEmpty());

    // Capture writer behavior.
    ArgumentCaptor<PubSubProducerCallback> putCallbackCaptor = ArgumentCaptor.forClass(PubSubProducerCallback.class);
    verify(writer.getVeniceWriter(), times(2)).put(any(), any(), eq(1), anyLong(), putCallbackCaptor.capture());

    // Verify final callback is ChainedPubSubCallback
    for (int i = 0; i < numberOfOperations; i++) {
      PubSubProducerCallback putCallback = putCallbackCaptor.getAllValues().get(i);
      Assert.assertTrue(putCallback instanceof CompletableFutureCallback);
      Assert
          .assertEquals(((CompletableFutureCallback) putCallback).getCompletableFuture(), completableFutureList.get(i));
    }
  }

  @Test
  public void testPutWithBatchSizeCheck() {
    List<ProducerBufferRecord> bufferRecordList = new ArrayList<>();
    Map<ByteBuffer, ProducerBufferRecord> bufferRecordIndex = new VeniceConcurrentHashMap<>();
    List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
    List<CompletableFutureCallback> completableFutureCallbackList = new ArrayList<>();
    int numberOfOperations = 2;
    BatchingVeniceWriter<String, GenericRecord, GenericRecord> writer = prepareMockSetup(
        numberOfOperations,
        completableFutureList,
        completableFutureCallbackList,
        bufferRecordIndex,
        bufferRecordList);

    String key = "a";

    GenericRecord valueRecord = new GenericData.Record(VALUE_SCHEMA);
    valueRecord.put("name", "J");
    valueRecord.put("age", 0);
    valueRecord.put("intArray", Collections.emptyList());
    valueRecord.put("recordArray", Collections.emptyList());
    valueRecord.put("stringMap", Collections.emptyMap());
    valueRecord.put("recordMap", Collections.emptyMap());
    // Next write should trigger the
    doReturn(1).when(writer).getMaxBatchSizeInBytes();
    doReturn(10).when(writer).getBufferSizeInBytes();
    writer.put(key, valueRecord, 1, completableFutureCallbackList.get(0));
    writer.put(key, valueRecord, 1, completableFutureCallbackList.get(1));

    // Perform produce operation
    verify(writer, times(2)).sendRecord(any());
    Assert.assertTrue(bufferRecordList.isEmpty());
    Assert.assertTrue(bufferRecordIndex.isEmpty());

    // Capture writer behavior.
    ArgumentCaptor<PubSubProducerCallback> putCallbackCaptor = ArgumentCaptor.forClass(PubSubProducerCallback.class);
    verify(writer.getVeniceWriter(), times(2)).put(any(), any(), eq(1), anyLong(), putCallbackCaptor.capture());

    // Verify final callback is ChainedPubSubCallback
    for (int i = 0; i < numberOfOperations; i++) {
      PubSubProducerCallback putCallback = putCallbackCaptor.getAllValues().get(i);
      Assert.assertTrue(putCallback instanceof CompletableFutureCallback);
      Assert
          .assertEquals(((CompletableFutureCallback) putCallback).getCompletableFuture(), completableFutureList.get(i));
    }
  }

  @Test
  public void testPutAndUpdate() {
    List<ProducerBufferRecord> bufferRecordList = new ArrayList<>();
    Map<ByteBuffer, ProducerBufferRecord> bufferRecordIndex = new VeniceConcurrentHashMap<>();
    List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
    List<CompletableFutureCallback> completableFutureCallbackList = new ArrayList<>();
    int numberOfOperations = 2;
    BatchingVeniceWriter<String, GenericRecord, GenericRecord> writer = prepareMockSetup(
        numberOfOperations,
        completableFutureList,
        completableFutureCallbackList,
        bufferRecordIndex,
        bufferRecordList);

    String key = "a";
    GenericRecord updateRecord = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", "K").build();
    byte[] updateBytes = updateSerializer.serialize(updateRecord);

    GenericRecord valueRecord = new GenericData.Record(VALUE_SCHEMA);
    valueRecord.put("name", "J");
    valueRecord.put("age", 0);
    valueRecord.put("intArray", Collections.emptyList());
    valueRecord.put("recordArray", Collections.emptyList());
    valueRecord.put("stringMap", Collections.emptyMap());
    valueRecord.put("recordMap", Collections.emptyMap());
    byte[] valueBytes = valueSerializer.serialize(valueRecord);
    writer.put(key, valueRecord, 1, completableFutureCallbackList.get(0));
    writer.update(key, updateRecord, 1, 1, completableFutureCallbackList.get(1));

    Assert.assertEquals(bufferRecordList.size(), numberOfOperations);
    Assert.assertFalse(bufferRecordIndex.isEmpty());

    Assert.assertEquals(bufferRecordList.get(0).getSerializedValue(), valueBytes);
    Assert.assertTrue(bufferRecordList.get(0).shouldSkipProduce());
    Assert.assertEquals(bufferRecordList.get(0).getTimestamp(), VeniceWriter.APP_DEFAULT_LOGICAL_TS);

    Assert.assertEquals(bufferRecordList.get(1).getSerializedUpdate(), updateBytes);
    Assert.assertFalse(bufferRecordList.get(1).shouldSkipProduce());
    Assert.assertEquals(bufferRecordList.get(1).getTimestamp(), VeniceWriter.APP_DEFAULT_LOGICAL_TS);

    // Perform produce operation
    writer.checkAndMaybeProduceBatchRecord();
    verify(writer, times(1)).sendRecord(any());
    Assert.assertTrue(bufferRecordList.isEmpty());
    Assert.assertTrue(bufferRecordIndex.isEmpty());

    // Capture writer behavior.
    ArgumentCaptor<PubSubProducerCallback> updateCallbackCaptor = ArgumentCaptor.forClass(PubSubProducerCallback.class);
    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(writer.getVeniceWriter(), times(1))
        .update(any(), payloadCaptor.capture(), eq(1), eq(1), updateCallbackCaptor.capture(), anyLong());

    // Verify produced merged UPDATE value.
    GenericRecord finalValue = updateDeserializer.deserialize(payloadCaptor.getValue());
    Assert.assertEquals(finalValue.get("age"), 0);
    Assert.assertEquals(finalValue.get("name").toString(), "K");

    // Verify final callback is ChainedPubSubCallback
    PubSubProducerCallback putCallback = updateCallbackCaptor.getValue();
    Assert.assertTrue(putCallback instanceof ChainedPubSubCallback);
    Assert.assertEquals(
        ((CompletableFutureCallback) ((ChainedPubSubCallback) putCallback).getDependentCallbackList().get(0))
            .getCompletableFuture(),
        completableFutureList.get(0));
    Assert.assertEquals(
        ((CompletableFutureCallback) ((ChainedPubSubCallback) putCallback).getCallback()).getCompletableFuture(),
        completableFutureList.get(1));

    // Verify ChainedPubSubCallback behavior
    putCallback.onCompletion(null, null);
    Assert.assertTrue(completableFutureList.get(0).isDone());
    Assert.assertTrue(completableFutureList.get(1).isDone());
  }

  @Test
  public void testUpdateAndUpdate() {
    List<ProducerBufferRecord> bufferRecordList = new ArrayList<>();
    Map<ByteBuffer, ProducerBufferRecord> bufferRecordIndex = new VeniceConcurrentHashMap<>();
    List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
    List<CompletableFutureCallback> completableFutureCallbackList = new ArrayList<>();
    int numberOfOperations = 2;
    BatchingVeniceWriter<String, GenericRecord, GenericRecord> writer = prepareMockSetup(
        numberOfOperations,
        completableFutureList,
        completableFutureCallbackList,
        bufferRecordIndex,
        bufferRecordList);

    String key = "a";
    List<Integer> intArray = Arrays.asList(1, 2, 3);
    GenericRecord updateRecord1 = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", "K")
        .setNewFieldValue("intArray", intArray)
        .build();
    byte[] updateBytes1 = updateSerializer.serialize(updateRecord1);

    List<Integer> intArrayUnion = Arrays.asList(3, 4);
    List<Integer> intArrayDiff = Collections.singletonList(2);

    GenericRecord updateRecord2 = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", "J")
        .setElementsToAddToListField("intArray", intArrayUnion)
        .setElementsToRemoveFromListField("intArray", intArrayDiff)
        .build();
    byte[] updateBytes2 = updateSerializer.serialize(updateRecord2);

    writer.update(key, updateRecord1, 1, 1, completableFutureCallbackList.get(0));
    writer.update(key, updateRecord2, 1, 1, completableFutureCallbackList.get(1));

    Assert.assertEquals(bufferRecordList.size(), numberOfOperations);
    Assert.assertFalse(bufferRecordIndex.isEmpty());

    Assert.assertEquals(bufferRecordList.get(0).getSerializedUpdate(), updateBytes1);
    Assert.assertTrue(bufferRecordList.get(0).shouldSkipProduce());
    Assert.assertEquals(bufferRecordList.get(0).getTimestamp(), VeniceWriter.APP_DEFAULT_LOGICAL_TS);

    Assert.assertEquals(bufferRecordList.get(1).getSerializedUpdate(), updateBytes2);
    Assert.assertFalse(bufferRecordList.get(1).shouldSkipProduce());
    Assert.assertEquals(bufferRecordList.get(1).getTimestamp(), VeniceWriter.APP_DEFAULT_LOGICAL_TS);

    // Perform produce operation
    writer.checkAndMaybeProduceBatchRecord();
    verify(writer, times(1)).sendRecord(any());
    Assert.assertTrue(bufferRecordList.isEmpty());
    Assert.assertTrue(bufferRecordIndex.isEmpty());

    // Capture writer behavior.
    ArgumentCaptor<PubSubProducerCallback> updateCallbackCaptor = ArgumentCaptor.forClass(PubSubProducerCallback.class);
    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(writer.getVeniceWriter(), times(1))
        .update(any(), payloadCaptor.capture(), eq(1), eq(1), updateCallbackCaptor.capture(), anyLong());

    // Verify produced merged UPDATE value.
    GenericRecord finalValue = updateDeserializer.deserialize(payloadCaptor.getValue());
    Assert.assertEquals(finalValue.get("name").toString(), "J");
    Assert.assertTrue(finalValue.get("intArray") instanceof List);

    List<Integer> intArrayField = (List<Integer>) finalValue.get("intArray");
    Assert.assertEquals(intArrayField.size(), 3);
    Assert.assertTrue(intArrayField.contains(1));
    Assert.assertTrue(intArrayField.contains(3));
    Assert.assertTrue(intArrayField.contains(4));

    // Verify final callback is ChainedPubSubCallback
    PubSubProducerCallback putCallback = updateCallbackCaptor.getValue();
    Assert.assertTrue(putCallback instanceof ChainedPubSubCallback);
    Assert.assertEquals(
        ((CompletableFutureCallback) ((ChainedPubSubCallback) putCallback).getDependentCallbackList().get(0))
            .getCompletableFuture(),
        completableFutureList.get(0));
    Assert.assertEquals(
        ((CompletableFutureCallback) ((ChainedPubSubCallback) putCallback).getCallback()).getCompletableFuture(),
        completableFutureList.get(1));

    // Verify ChainedPubSubCallback behavior
    putCallback.onCompletion(null, null);
    Assert.assertTrue(completableFutureList.get(0).isDone());
    Assert.assertTrue(completableFutureList.get(1).isDone());
  }

  BatchingVeniceWriter<String, GenericRecord, GenericRecord> prepareMockSetup(
      int numberOfOperations,
      List<CompletableFuture<Void>> completableFutureList,
      List<CompletableFutureCallback> completableFutureCallbackList,
      Map<ByteBuffer, ProducerBufferRecord> bufferRecordIndex,
      List<ProducerBufferRecord> bufferRecordList) {
    SchemaFetcherBackedStoreSchemaCache storeSchemaCache = mock(SchemaFetcherBackedStoreSchemaCache.class);
    doReturn(1).when(storeSchemaCache).getLatestOrSupersetSchemaId();
    doReturn(UPDATE_SCHEMA).when(storeSchemaCache).getUpdateSchema();
    doReturn(VALUE_SCHEMA).when(storeSchemaCache).getValueSchema(1);
    doReturn(VALUE_SCHEMA).when(storeSchemaCache).getSupersetSchema();

    BatchingVeniceWriter<String, GenericRecord, GenericRecord> writer = mock(BatchingVeniceWriter.class);
    VeniceWriter<byte[], byte[], byte[]> internalWriter = mock(VeniceWriter.class);
    doReturn(internalWriter).when(writer).getVeniceWriter();
    VeniceKafkaSerializer keySerializer = new StringSerializer();
    doReturn(keySerializer).when(writer).getKeySerializer();
    doReturn(new VeniceAvroKafkaSerializer(UPDATE_SCHEMA.toString())).when(writer).getUpdateSerializer();
    doReturn(new VeniceAvroKafkaSerializer(VALUE_SCHEMA.toString())).when(writer).getValueSerializer();
    doReturn("abc").when(writer).getTopicName();
    doReturn(storeSchemaCache).when(writer).getStoreSchemaCache();

    bufferRecordList.clear();
    bufferRecordIndex.clear();
    doReturn(bufferRecordIndex).when(writer).getBufferRecordIndex();
    doReturn(bufferRecordList).when(writer).getBufferRecordList();
    doCallRealMethod().when(writer).addRecordToBuffer(any(), any(), any(), any(), anyInt(), anyInt(), any(), anyLong());
    doCallRealMethod().when(writer).checkAndMaybeProduceBatchRecord();
    doCallRealMethod().when(writer).sendRecord(any());
    doCallRealMethod().when(writer).put(any(), any(), anyInt(), any());
    doCallRealMethod().when(writer).put(any(), any(), anyInt(), anyLong(), any());
    doCallRealMethod().when(writer).delete(any(), any());
    doCallRealMethod().when(writer).delete(any(), anyLong(), any());
    doCallRealMethod().when(writer).update(any(), any(), anyInt(), anyInt(), any());
    doCallRealMethod().when(writer).update(any(), any(), anyInt(), anyInt(), anyLong(), any());
    doCallRealMethod().when(writer).maybeUpdateRecordUpdatePayload(any());
    doCallRealMethod().when(writer).mergeAndProduceWithSizeLimit(any(), anyInt(), any(), anyInt());
    doCallRealMethod().when(writer).convertValueRecordToUpdateRecord(any(), any());
    doReturn(VeniceWriter.DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES).when(writer)
        .getMaxSizeForUserPayloadPerMessageInBytes();
    doReturn(valueDeserializer).when(writer).getValueDeserializer(anyInt(), anyInt());
    doReturn(updateHandler).when(writer).getUpdateHandler();

    ReentrantLock lock = new ReentrantLock();
    doReturn(lock).when(writer).getLock();
    // Set to a big number to avoid direct produce.
    doReturn(100000).when(writer).getMaxBatchSizeInBytes();

    completableFutureList.clear();
    completableFutureCallbackList.clear();

    for (int i = 0; i < numberOfOperations; i++) {
      CompletableFuture<Void> completableFuture = new CompletableFuture<>();
      CompletableFutureCallback completableFutureCallback = new CompletableFutureCallback(completableFuture);
      completableFutureList.add(completableFuture);
      completableFutureCallbackList.add(completableFutureCallback);
    }
    return writer;
  }

  @Test
  public void testMergedUpdateExceedsSizeLimitProducesIntermediateBatch() {
    List<ProducerBufferRecord> bufferRecordList = new ArrayList<>();
    Map<ByteBuffer, ProducerBufferRecord> bufferRecordIndex = new VeniceConcurrentHashMap<>();
    List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
    List<CompletableFutureCallback> completableFutureCallbackList = new ArrayList<>();
    int numberOfOperations = 3;
    BatchingVeniceWriter<String, GenericRecord, GenericRecord> writer = prepareMockSetup(
        numberOfOperations,
        completableFutureList,
        completableFutureCallbackList,
        bufferRecordIndex,
        bufferRecordList);

    // Each update touches a DIFFERENT field so the merged result grows larger than any individual update.
    // We use a large string for name, a large map for stringMap, and a large array for intArray.
    StringBuilder largeName = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      largeName.append("abcdefghij"); // 1000 chars
    }
    Map<String, String> largeMap = new java.util.HashMap<>();
    for (int i = 0; i < 50; i++) {
      largeMap.put("key_" + i, "value_" + i);
    }
    List<Integer> largeIntArray = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      largeIntArray.add(i);
    }

    // Each individual update is ~500-1000 bytes, but merging all 3 produces ~2500+ bytes.
    // Set limit so individual fits but merged-all exceeds.
    doReturn(2000).when(writer).getMaxSizeForUserPayloadPerMessageInBytes();

    String key = "a";
    GenericRecord updateRecord1 =
        new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", largeName.toString()).build();
    GenericRecord updateRecord2 = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("stringMap", largeMap).build();
    GenericRecord updateRecord3 =
        new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("intArray", largeIntArray).build();

    writer.update(key, updateRecord1, 1, 1, completableFutureCallbackList.get(0));
    writer.update(key, updateRecord2, 1, 1, completableFutureCallbackList.get(1));
    writer.update(key, updateRecord3, 1, 1, completableFutureCallbackList.get(2));

    Assert.assertEquals(bufferRecordList.size(), numberOfOperations);

    // Perform produce operation — should trigger size-aware splitting
    writer.checkAndMaybeProduceBatchRecord();

    // The merged result of all 3 exceeds the limit, so intermediate produces should occur.
    // Verify more than 1 update call was made to the internal writer (intermediate + final).
    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<PubSubProducerCallback> callbackCaptor = ArgumentCaptor.forClass(PubSubProducerCallback.class);
    // sendRecord is called once for the final record; intermediate produces go directly to VeniceWriter
    verify(writer, times(1)).sendRecord(any());
    // Internal writer should have received at least 2 update calls (intermediate + final via sendRecord)
    verify(writer.getVeniceWriter(), atLeast(2))
        .update(any(), payloadCaptor.capture(), eq(1), eq(1), callbackCaptor.capture(), anyLong());

    // Verify all callbacks are eventually completable (invoke onCompletion on captured callbacks)
    for (PubSubProducerCallback cb: callbackCaptor.getAllValues()) {
      cb.onCompletion(null, null);
    }
    for (CompletableFuture<Void> future: completableFutureList) {
      Assert.assertTrue(future.isDone());
    }
  }

  @Test
  public void testMergedUpdateUnderSizeLimitStillMerges() {
    List<ProducerBufferRecord> bufferRecordList = new ArrayList<>();
    Map<ByteBuffer, ProducerBufferRecord> bufferRecordIndex = new VeniceConcurrentHashMap<>();
    List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
    List<CompletableFutureCallback> completableFutureCallbackList = new ArrayList<>();
    int numberOfOperations = 2;
    BatchingVeniceWriter<String, GenericRecord, GenericRecord> writer = prepareMockSetup(
        numberOfOperations,
        completableFutureList,
        completableFutureCallbackList,
        bufferRecordIndex,
        bufferRecordList);

    // Default max payload size is large enough — merge should work normally
    String key = "a";
    GenericRecord updateRecord1 = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", "Alice").build();
    GenericRecord updateRecord2 = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("age", 30).build();

    writer.update(key, updateRecord1, 1, 1, completableFutureCallbackList.get(0));
    writer.update(key, updateRecord2, 1, 1, completableFutureCallbackList.get(1));

    Assert.assertEquals(bufferRecordList.size(), numberOfOperations);

    writer.checkAndMaybeProduceBatchRecord();

    // Should produce exactly once with merged payload (normal path, no splitting)
    verify(writer, times(1)).sendRecord(any());
    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(writer.getVeniceWriter(), times(1)).update(any(), payloadCaptor.capture(), eq(1), eq(1), any(), anyLong());

    // Verify merged payload contains both fields
    GenericRecord finalValue = updateDeserializer.deserialize(payloadCaptor.getValue());
    Assert.assertEquals(finalValue.get("name").toString(), "Alice");
    Assert.assertEquals(finalValue.get("age"), 30);
  }

  @Test
  public void testMergedUpdateWithPutAnchorExceedsSizeLimit() {
    List<ProducerBufferRecord> bufferRecordList = new ArrayList<>();
    Map<ByteBuffer, ProducerBufferRecord> bufferRecordIndex = new VeniceConcurrentHashMap<>();
    List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
    List<CompletableFutureCallback> completableFutureCallbackList = new ArrayList<>();
    int numberOfOperations = 3;
    BatchingVeniceWriter<String, GenericRecord, GenericRecord> writer = prepareMockSetup(
        numberOfOperations,
        completableFutureList,
        completableFutureCallbackList,
        bufferRecordIndex,
        bufferRecordList);

    // PUT sets all fields with a large stringMap, updates touch different fields to grow the merge
    Map<String, String> largeMap = new java.util.HashMap<>();
    for (int i = 0; i < 50; i++) {
      largeMap.put("key_" + i, "value_" + i);
    }
    StringBuilder largeName = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      largeName.append("abcdefghij");
    }
    List<Integer> largeIntArray = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      largeIntArray.add(i);
    }

    // Set max payload size so individual fits but merged-all exceeds
    doReturn(2000).when(writer).getMaxSizeForUserPayloadPerMessageInBytes();

    String key = "a";
    GenericRecord valueRecord = new GenericData.Record(VALUE_SCHEMA);
    valueRecord.put("name", "J");
    valueRecord.put("age", 0);
    valueRecord.put("intArray", Collections.emptyList());
    valueRecord.put("recordArray", Collections.emptyList());
    valueRecord.put("stringMap", largeMap);
    valueRecord.put("recordMap", Collections.emptyMap());

    // Update1 sets name to a large string, Update2 sets intArray to large array
    // When PUT (converted to UPDATE) is merged with both, all fields are large → exceeds limit
    GenericRecord updateRecord1 =
        new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", largeName.toString()).build();
    GenericRecord updateRecord2 =
        new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("intArray", largeIntArray).build();

    writer.put(key, valueRecord, 1, completableFutureCallbackList.get(0));
    writer.update(key, updateRecord1, 1, 1, completableFutureCallbackList.get(1));
    writer.update(key, updateRecord2, 1, 1, completableFutureCallbackList.get(2));

    Assert.assertEquals(bufferRecordList.size(), numberOfOperations);

    writer.checkAndMaybeProduceBatchRecord();

    // Should have produced intermediate batch(es) + final via sendRecord
    verify(writer, times(1)).sendRecord(any());
    // Internal writer should have multiple update calls
    ArgumentCaptor<PubSubProducerCallback> callbackCaptor = ArgumentCaptor.forClass(PubSubProducerCallback.class);
    verify(writer.getVeniceWriter(), atLeast(2))
        .update(any(), any(), eq(1), eq(1), callbackCaptor.capture(), anyLong());

    // Verify all callbacks can be completed
    for (PubSubProducerCallback cb: callbackCaptor.getAllValues()) {
      cb.onCompletion(null, null);
    }
    for (CompletableFuture<Void> future: completableFutureList) {
      Assert.assertTrue(future.isDone());
    }
  }

  @Test
  public void testDeduplicatedWritesProduceOncePerKeyAndHookFiresOnce() {
    VeniceWriterHook mockHook = mock(VeniceWriterHook.class);
    PubSubProducerAdapter mockProducer = mock(PubSubProducerAdapter.class);
    doReturn(mock(CompletableFuture.class)).when(mockProducer).sendMessage(any(), any(), any(), any(), any(), any());

    // Create a real internal writer with the hook so hook calls can be verified
    VeniceWriter<byte[], byte[], byte[]> realInternalWriter = new VeniceWriter<>(
        new VeniceWriterOptions.Builder("abc").setPartitionCount(1).setWriterHook(mockHook).build(),
        VeniceProperties.empty(),
        mockProducer);

    List<ProducerBufferRecord> bufferRecordList = new ArrayList<>();
    Map<ByteBuffer, ProducerBufferRecord> bufferRecordIndex = new VeniceConcurrentHashMap<>();
    List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
    List<CompletableFutureCallback> completableFutureCallbackList = new ArrayList<>();
    BatchingVeniceWriter<String, GenericRecord, GenericRecord> writer =
        prepareMockSetup(6, completableFutureList, completableFutureCallbackList, bufferRecordIndex, bufferRecordList);
    doReturn(realInternalWriter).when(writer).getVeniceWriter();

    GenericRecord valueRecord = new GenericData.Record(VALUE_SCHEMA);
    valueRecord.put("name", "J");
    valueRecord.put("age", 0);
    valueRecord.put("intArray", Collections.emptyList());
    valueRecord.put("recordArray", Collections.emptyList());
    valueRecord.put("stringMap", Collections.emptyMap());
    valueRecord.put("recordMap", Collections.emptyMap());

    // PUT same key twice — only the second should be produced
    writer.put("putKey", valueRecord, 1, completableFutureCallbackList.get(0));
    writer.put("putKey", valueRecord, 1, completableFutureCallbackList.get(1));

    // DELETE same key twice — only the second should be produced
    writer.delete("deleteKey", completableFutureCallbackList.get(2));
    writer.delete("deleteKey", completableFutureCallbackList.get(3));

    // UPDATE same key twice — only the merged result should be produced
    GenericRecord updateRecord = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", "K").build();
    writer.update("updateKey", updateRecord, 1, 1, completableFutureCallbackList.get(4));
    writer.update("updateKey", updateRecord, 1, 1, completableFutureCallbackList.get(5));

    Assert.assertEquals(bufferRecordList.size(), 6);
    writer.checkAndMaybeProduceBatchRecord();

    // Each key should produce exactly once — sendRecord called 3 times (one per unique key)
    verify(writer, times(3)).sendRecord(any());

    // Hook should fire exactly once per unique key with correct operation type
    verify(mockHook).onBeforeProduce(eq(VeniceWriterHook.OperationType.PUT), anyInt(), anyInt());
    verify(mockHook).onBeforeProduce(eq(VeniceWriterHook.OperationType.DELETE), anyInt(), eq(0));
    verify(mockHook).onBeforeProduce(eq(VeniceWriterHook.OperationType.UPDATE), anyInt(), anyInt());
  }
}
