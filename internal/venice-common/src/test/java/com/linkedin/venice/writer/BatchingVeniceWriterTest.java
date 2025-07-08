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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BatchingVeniceWriterTest {
  @Test
  public void testSendRecord() {
    BatchingVeniceWriter<byte[], byte[], byte[]> writer = mock(BatchingVeniceWriter.class);
    doCallRealMethod().when(writer).sendRecord(any());
    byte[] keyBytes = "abc".getBytes();
    byte[] valueBytes = "def".getBytes();
    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    CompletableFutureCallback completableFutureCallback = new CompletableFutureCallback(completableFuture);
    int valueSchemaId = 100;
    int protocolId = 200;
    long logicalTimestamp = 10000L;

    ProducerBufferRecord<byte[], byte[], byte[]> putRecord = new ProducerBufferRecord<>(
        MessageType.PUT,
        keyBytes,
        valueBytes,
        null,
        valueSchemaId,
        protocolId,
        completableFutureCallback,
        logicalTimestamp);
    writer.sendRecord(putRecord);
    verify(writer, times(1)).internalPut(eq(keyBytes), eq(valueBytes), eq(valueSchemaId), eq(logicalTimestamp), any());

    ProducerBufferRecord<byte[], byte[], byte[]> deleteRecord = new ProducerBufferRecord<>(
        MessageType.DELETE,
        keyBytes,
        null,
        null,
        valueSchemaId,
        protocolId,
        completableFutureCallback,
        logicalTimestamp);
    writer.sendRecord(deleteRecord);
    verify(writer, times(1)).internalDelete(eq(keyBytes), eq(logicalTimestamp), any());

    ProducerBufferRecord<byte[], byte[], byte[]> updateRecord = new ProducerBufferRecord<>(
        MessageType.UPDATE,
        keyBytes,
        null,
        valueBytes,
        valueSchemaId,
        protocolId,
        completableFutureCallback,
        logicalTimestamp);
    writer.sendRecord(updateRecord);
    verify(writer, times(1))
        .internalUpdate(eq(keyBytes), eq(valueBytes), eq(valueSchemaId), eq(protocolId), eq(logicalTimestamp), any());

  }

  @Test
  public void testAddRecordAndBatchProduce() {
    BatchingVeniceWriter<byte[], byte[], byte[]> writer = mock(BatchingVeniceWriter.class);
    List<ProducerBufferRecord<byte[], byte[], byte[]>> bufferRecordList = new ArrayList<>();
    Comparator<byte[]> comparator = Comparator.comparing(ByteBuffer::wrap);
    Map<byte[], ProducerBufferRecord<byte[], byte[], byte[]>> bufferRecordIndex = new TreeMap<>(comparator);
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
    doCallRealMethod().when(writer).update(any(), any(), anyInt(), anyInt(), any(), anyLong());
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
    callback1.setCallback(mock(PubSubProducerCallback.class));
    callback4.setCallback(mock(PubSubProducerCallback.class));

    // Create different objects for each message to make sure buffer index map is working properly for byte array.
    byte[] keyBytes1 = "a".getBytes();
    byte[] keyBytes2 = "a".getBytes();
    byte[] keyBytes3 = "a".getBytes();
    byte[] keyBytes4 = "a".getBytes();

    byte[] valueBytes2 = "2".getBytes();
    byte[] valueBytes3 = "3".getBytes();
    byte[] valueBytes4 = "4".getBytes();

    int valueSchemaId = 100;
    int protocolId = 200;
    long logicalTimestamp = 10000L;

    writer.delete(keyBytes1, callback1);
    writer.update(keyBytes2, valueBytes2, valueSchemaId, protocolId, callback2);
    writer.put(keyBytes3, valueBytes3, valueSchemaId, logicalTimestamp, callback3);
    writer.put(keyBytes4, valueBytes4, valueSchemaId, callback4);
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
    Assert.assertEquals(bufferRecordIndex.get(keyBytes1).getValue(), valueBytes4);

    // Perform produce operation
    writer.checkAndMaybeProduceBatchRecord();
    verify(writer, times(3)).sendRecord(any());
    Assert.assertTrue(bufferRecordList.isEmpty());
    Assert.assertTrue(bufferRecordIndex.isEmpty());

    // Complete callback and validate dependent callback is also completed.
    verify(writer, times(0)).internalDelete(eq(keyBytes1), anyLong(), any());

    ArgumentCaptor<PubSubProducerCallback> updateCallbackCaptor = ArgumentCaptor.forClass(PubSubProducerCallback.class);
    verify(writer, times(1)).internalUpdate(
        eq(keyBytes1),
        eq(valueBytes2),
        eq(valueSchemaId),
        eq(protocolId),
        eq(VeniceWriter.APP_DEFAULT_LOGICAL_TS),
        updateCallbackCaptor.capture());

    PubSubProducerCallback updateCallback = updateCallbackCaptor.getValue();
    Assert.assertTrue(updateCallback instanceof CompletableFutureCallback);
    Assert.assertEquals(((CompletableFutureCallback) updateCallback).getCompletableFuture(), completableFuture2);

    ArgumentCaptor<PubSubProducerCallback> putCallbackCaptor = ArgumentCaptor.forClass(PubSubProducerCallback.class);
    verify(writer, times(2)).internalPut(any(), any(), eq(valueSchemaId), anyLong(), putCallbackCaptor.capture());
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
    writer.delete(keyBytes1, callback1);
    verify(writer, times(4)).sendRecord(any());
    verify(writer, times(1)).internalDelete(eq(keyBytes1), anyLong(), any());
  }
}
