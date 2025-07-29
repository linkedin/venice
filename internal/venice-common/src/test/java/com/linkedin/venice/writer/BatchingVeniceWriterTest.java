package com.linkedin.venice.writer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
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
    doCallRealMethod().when(writer).convertValueRecordToUpdateRecord(any(), any());
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
}
