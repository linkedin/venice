package com.linkedin.venice.samza;

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
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.CompletableFutureCallback;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ProducerBatchingServiceTest {
  @Test
  public void testSendRecord() {
    ProducerBatchingService producerBatchingService = mock(ProducerBatchingService.class);
    VeniceWriter writer = mock(VeniceWriter.class);
    doReturn(writer).when(producerBatchingService).getWriter();
    doCallRealMethod().when(producerBatchingService).sendRecord(any());
    byte[] keyBytes = "abc".getBytes();
    byte[] valueBytes = "def".getBytes();
    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    int valueSchemaId = 100;
    int protocolId = 200;
    long logicalTimestamp = 10000L;

    ProducerBufferRecord putRecord = new ProducerBufferRecord(
        MessageType.PUT,
        keyBytes,
        valueBytes,
        valueSchemaId,
        protocolId,
        completableFuture,
        logicalTimestamp);
    producerBatchingService.sendRecord(putRecord);
    verify(writer, times(1)).put(eq(keyBytes), eq(valueBytes), eq(valueSchemaId), eq(logicalTimestamp), any());

    ProducerBufferRecord deleteRecord = new ProducerBufferRecord(
        MessageType.DELETE,
        keyBytes,
        valueBytes,
        valueSchemaId,
        protocolId,
        completableFuture,
        logicalTimestamp);
    producerBatchingService.sendRecord(deleteRecord);
    verify(writer, times(1)).delete(eq(keyBytes), eq(logicalTimestamp), any());

    ProducerBufferRecord updateRecord = new ProducerBufferRecord(
        MessageType.UPDATE,
        keyBytes,
        valueBytes,
        valueSchemaId,
        protocolId,
        completableFuture,
        logicalTimestamp);
    producerBatchingService.sendRecord(updateRecord);
    verify(writer, times(1))
        .update(eq(keyBytes), eq(valueBytes), eq(valueSchemaId), eq(protocolId), any(), eq(logicalTimestamp));
  }

  @Test
  public void testAddRecordAndBatchProduce() {
    ProducerBatchingService producerBatchingService = mock(ProducerBatchingService.class);
    VeniceWriter writer = mock(VeniceWriter.class);
    doReturn(writer).when(producerBatchingService).getWriter();
    List<ProducerBufferRecord> bufferRecordList = new ArrayList<>();
    Map<ByteBuffer, ProducerBufferRecord> bufferRecordIndex = new VeniceConcurrentHashMap<>();
    doReturn(bufferRecordIndex).when(producerBatchingService).getBufferRecordIndex();
    doReturn(bufferRecordList).when(producerBatchingService).getBufferRecordList();
    doCallRealMethod().when(producerBatchingService).addRecordToBuffer(any(), any(), any(), anyInt(), anyInt(), any());
    doCallRealMethod().when(producerBatchingService)
        .addRecordToBuffer(any(), any(), any(), anyInt(), anyInt(), any(), anyLong());
    doCallRealMethod().when(producerBatchingService).checkAndMaybeProduceBatchRecord();
    doCallRealMethod().when(producerBatchingService).sendRecord(any());
    ReentrantLock lock = new ReentrantLock();
    doReturn(lock).when(producerBatchingService).getLock();
    doReturn(5).when(producerBatchingService).getMaxBatchSize();

    CompletableFuture<Void> completableFuture1 = new CompletableFuture<>();
    CompletableFuture<Void> completableFuture2 = new CompletableFuture<>();
    CompletableFuture<Void> completableFuture3 = new CompletableFuture<>();
    CompletableFuture<Void> completableFuture4 = new CompletableFuture<>();

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

    producerBatchingService.addRecordToBuffer(MessageType.DELETE, keyBytes1, null, -1, -1, completableFuture1);
    producerBatchingService
        .addRecordToBuffer(MessageType.UPDATE, keyBytes2, valueBytes2, valueSchemaId, protocolId, completableFuture2);
    producerBatchingService.addRecordToBuffer(
        MessageType.PUT,
        keyBytes3,
        valueBytes3,
        valueSchemaId,
        -1,
        completableFuture3,
        logicalTimestamp);
    producerBatchingService
        .addRecordToBuffer(MessageType.PUT, keyBytes4, valueBytes4, valueSchemaId, -1, completableFuture4);
    Assert.assertEquals(bufferRecordList.size(), 4);
    Assert.assertFalse(bufferRecordIndex.isEmpty());
    Assert.assertEquals(bufferRecordList.get(0).getValueBytes(), null);
    Assert.assertTrue(bufferRecordList.get(0).shouldSkipProduce());
    Assert.assertEquals(bufferRecordList.get(0).getLogicalTimestamp(), VeniceWriter.APP_DEFAULT_LOGICAL_TS);

    Assert.assertEquals(bufferRecordList.get(1).getValueBytes(), valueBytes2);
    Assert.assertFalse(bufferRecordList.get(1).shouldSkipProduce());
    Assert.assertEquals(bufferRecordList.get(1).getLogicalTimestamp(), VeniceWriter.APP_DEFAULT_LOGICAL_TS);

    Assert.assertEquals(bufferRecordList.get(2).getValueBytes(), valueBytes3);
    Assert.assertFalse(bufferRecordList.get(2).shouldSkipProduce());
    Assert.assertEquals(bufferRecordList.get(2).getLogicalTimestamp(), logicalTimestamp);

    Assert.assertEquals(bufferRecordList.get(3).getValueBytes(), valueBytes4);
    Assert.assertFalse(bufferRecordList.get(3).shouldSkipProduce());
    Assert.assertEquals(bufferRecordList.get(3).getLogicalTimestamp(), VeniceWriter.APP_DEFAULT_LOGICAL_TS);

    // Index should only contain 1 mapping.
    Assert.assertEquals(bufferRecordIndex.get(ByteBuffer.wrap(keyBytes1)).getValueBytes(), valueBytes4);

    // Perform produce operation
    producerBatchingService.checkAndMaybeProduceBatchRecord();
    verify(producerBatchingService, times(3)).sendRecord(any());
    Assert.assertTrue(bufferRecordList.isEmpty());
    Assert.assertTrue(bufferRecordIndex.isEmpty());

    // Complete callback and validate dependent callback is also completed.
    verify(writer, times(0)).delete(eq(keyBytes1), anyLong(), any());
    ArgumentCaptor<CompletableFutureCallback> updateCallbackCaptor =
        ArgumentCaptor.forClass(CompletableFutureCallback.class);
    verify(writer, times(1)).update(
        eq(keyBytes1),
        eq(valueBytes2),
        eq(valueSchemaId),
        eq(protocolId),
        updateCallbackCaptor.capture(),
        eq(VeniceWriter.APP_DEFAULT_LOGICAL_TS));
    CompletableFutureCallback updateCallback = updateCallbackCaptor.getValue();
    Assert.assertTrue(updateCallback.getDependentFutureList().isEmpty());
    Assert.assertEquals(updateCallback.getCompletableFuture(), completableFuture2);

    ArgumentCaptor<CompletableFutureCallback> putCallbackCaptor =
        ArgumentCaptor.forClass(CompletableFutureCallback.class);
    verify(writer, times(2)).put(any(), any(), eq(valueSchemaId), anyLong(), putCallbackCaptor.capture());
    List<CompletableFutureCallback> putCallbacks = putCallbackCaptor.getAllValues();
    Assert.assertTrue(putCallbacks.get(0).getDependentFutureList().isEmpty());
    Assert.assertEquals(putCallbacks.get(0).getCompletableFuture(), completableFuture3);
    Assert.assertEquals(putCallbacks.get(1).getDependentFutureList().size(), 1);
    Assert.assertEquals(putCallbacks.get(1).getDependentFutureList().get(0), completableFuture1);
    Assert.assertEquals(putCallbacks.get(1).getCompletableFuture(), completableFuture4);

    putCallbacks.get(1).setCallback(mock(PubSubProducerCallback.class));
    putCallbacks.get(1).onCompletion(null, null);
    Assert.assertTrue(completableFuture1.isDone());
    Assert.assertTrue(completableFuture4.isDone());

    // Validate max batch size feature.
    doReturn(1).when(producerBatchingService).getMaxBatchSize();
    producerBatchingService.addRecordToBuffer(MessageType.DELETE, keyBytes1, null, -1, -1, completableFuture1);
    verify(producerBatchingService, times(4)).sendRecord(any());
    verify(writer, times(1)).delete(eq(keyBytes1), anyLong(), any());
  }
}
