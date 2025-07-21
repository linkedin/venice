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

import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.serialization.StringSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BatchingVeniceWriterTest {
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

    ProducerBufferRecord<byte[], byte[]> putRecord = new ProducerBufferRecord<>(
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

    ProducerBufferRecord<byte[], byte[]> deleteRecord = new ProducerBufferRecord<>(
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

    ProducerBufferRecord<byte[], byte[]> updateRecord = new ProducerBufferRecord<>(
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
  public void testAddRecordAndBatchProduce() {
    BatchingVeniceWriter<String, byte[], byte[]> writer = mock(BatchingVeniceWriter.class);
    VeniceWriter<byte[], byte[], byte[]> internalWriter = mock(VeniceWriter.class);
    doReturn(internalWriter).when(writer).getVeniceWriter();
    VeniceKafkaSerializer keySerializer = new StringSerializer();
    doReturn(keySerializer).when(writer).getKeySerializer();
    doReturn("abc").when(writer).getTopicName();
    List<ProducerBufferRecord<byte[], byte[]>> bufferRecordList = new ArrayList<>();
    Map<ByteBuffer, ProducerBufferRecord<byte[], byte[]>> bufferRecordIndex = new VeniceConcurrentHashMap<>();
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
    ReentrantLock lock = new ReentrantLock();
    doReturn(lock).when(writer).getLock();
    doReturn(100000).when(writer).getMaxBatchSizeInBytes();

    CompletableFuture<Void> completableFuture1 = new CompletableFuture<>();
    CompletableFuture<Void> completableFuture2 = new CompletableFuture<>();
    CompletableFuture<Void> completableFuture3 = new CompletableFuture<>();
    CompletableFuture<Void> completableFuture4 = new CompletableFuture<>();
    CompletableFutureCallback callback1 = new CompletableFutureCallback(completableFuture1);
    CompletableFutureCallback callback2 = new CompletableFutureCallback(completableFuture2);
    CompletableFutureCallback callback3 = new CompletableFutureCallback(completableFuture3);
    CompletableFutureCallback callback4 = new CompletableFutureCallback(completableFuture4);
    callback1.setInternalCallback(mock(PubSubProducerCallback.class));
    callback4.setInternalCallback(mock(PubSubProducerCallback.class));

    // Create different objects for each message to make sure buffer index map is working properly for byte array.
    String key = "a";
    byte[] serializedKey = key.getBytes();

    byte[] valueBytes2 = "2".getBytes();
    byte[] valueBytes3 = "3".getBytes();
    byte[] valueBytes4 = "4".getBytes();

    int valueSchemaId = 100;
    int protocolId = 200;
    long logicalTimestamp = 10000L;

    writer.delete(key, callback1);
    writer.update(key, valueBytes2, valueSchemaId, protocolId, callback2);
    writer.put(key, valueBytes3, valueSchemaId, logicalTimestamp, callback3);
    writer.put(key, valueBytes4, valueSchemaId, callback4);
    Assert.assertEquals(bufferRecordList.size(), 4);
    Assert.assertFalse(bufferRecordIndex.isEmpty());
    Assert.assertNull(bufferRecordList.get(0).getValue());
    Assert.assertTrue(bufferRecordList.get(0).shouldSkipProduce());
    Assert.assertEquals(bufferRecordList.get(0).getLogicalTimestamp(), VeniceWriter.APP_DEFAULT_LOGICAL_TS);

    Assert.assertEquals(bufferRecordList.get(1).getUpdate(), valueBytes2);
    Assert.assertFalse(bufferRecordList.get(1).shouldSkipProduce());
    Assert.assertEquals(bufferRecordList.get(1).getLogicalTimestamp(), VeniceWriter.APP_DEFAULT_LOGICAL_TS);

    Assert.assertEquals(bufferRecordList.get(2).getValue(), valueBytes3);
    Assert.assertFalse(bufferRecordList.get(2).shouldSkipProduce());
    Assert.assertEquals(bufferRecordList.get(2).getLogicalTimestamp(), logicalTimestamp);

    Assert.assertEquals(bufferRecordList.get(3).getValue(), valueBytes4);
    Assert.assertFalse(bufferRecordList.get(3).shouldSkipProduce());
    Assert.assertEquals(bufferRecordList.get(3).getLogicalTimestamp(), VeniceWriter.APP_DEFAULT_LOGICAL_TS);

    // Index should only contain 1 mapping.
    Assert.assertEquals(bufferRecordIndex.get(ByteBuffer.wrap(serializedKey)).getValue(), valueBytes4);

    // Perform produce operation
    writer.checkAndMaybeProduceBatchRecord();
    verify(writer, times(3)).sendRecord(any());
    Assert.assertTrue(bufferRecordList.isEmpty());
    Assert.assertTrue(bufferRecordIndex.isEmpty());

    // Complete callback and validate dependent callback is also completed.
    verify(internalWriter, times(0)).delete(eq(serializedKey), anyLong(), any());

    ArgumentCaptor<PubSubProducerCallback> updateCallbackCaptor = ArgumentCaptor.forClass(PubSubProducerCallback.class);
    verify(internalWriter, times(1)).update(
        eq(serializedKey),
        eq(valueBytes2),
        eq(valueSchemaId),
        eq(protocolId),
        updateCallbackCaptor.capture(),
        eq(VeniceWriter.APP_DEFAULT_LOGICAL_TS));

    PubSubProducerCallback updateCallback = updateCallbackCaptor.getValue();
    Assert.assertTrue(updateCallback instanceof CompletableFutureCallback);
    Assert.assertEquals(((CompletableFutureCallback) updateCallback).getCompletableFuture(), completableFuture2);

    ArgumentCaptor<PubSubProducerCallback> putCallbackCaptor = ArgumentCaptor.forClass(PubSubProducerCallback.class);
    verify(internalWriter, times(2)).put(any(), any(), eq(valueSchemaId), anyLong(), putCallbackCaptor.capture());
    List<PubSubProducerCallback> putCallbacks = putCallbackCaptor.getAllValues();
    Assert.assertTrue(putCallbacks.get(0) instanceof CompletableFutureCallback);
    Assert.assertEquals(((CompletableFutureCallback) putCallbacks.get(0)).getCompletableFuture(), completableFuture3);

    Assert.assertTrue(putCallbacks.get(1) instanceof ChainedPubSubCallback);
    ChainedPubSubCallback chainedPubSubCallback = (ChainedPubSubCallback) putCallbacks.get(1);
    Assert.assertEquals(chainedPubSubCallback.getDependentCallbackList().size(), 1);
    Assert.assertEquals(
        ((CompletableFutureCallback) (chainedPubSubCallback.getDependentCallbackList().get(0))).getCompletableFuture(),
        completableFuture1);
    Assert.assertEquals(
        ((CompletableFutureCallback) chainedPubSubCallback.getCallback()).getCompletableFuture(),
        completableFuture4);

    chainedPubSubCallback.onCompletion(null, null);
    Assert.assertTrue(completableFuture1.isDone());
    Assert.assertTrue(completableFuture4.isDone());

    // Validate max batch size feature.
    doReturn(1).when(writer).getMaxBatchSizeInBytes();
    doReturn(10).when(writer).getBufferSizeInBytes();
    writer.delete(key, callback1);
    verify(writer, times(4)).sendRecord(any());
    verify(internalWriter, times(1)).delete(eq(serializedKey), anyLong(), any());
  }
}
