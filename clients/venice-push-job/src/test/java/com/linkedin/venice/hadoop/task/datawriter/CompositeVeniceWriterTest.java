package com.linkedin.venice.hadoop.task.datawriter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.ComplexVeniceWriter;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.PutMetadata;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.avro.generic.GenericRecord;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CompositeVeniceWriterTest {
  private static final String TEST_VIEW_TOPIC_NAME = "testStore_v1_compositeTestView_mv";

  // Dealing with completable futures in this unit test, adding test timeout to avoid waiting indefinitely.
  private static final long TEST_TIMEOUT = 10000;
  private final BiFunction<byte[], Integer, GenericRecord> defaultValueExtractor = (valueBytes, valueSchemaId) -> null;

  @Test(timeOut = TEST_TIMEOUT)
  public void testChildWriteExceptions() {
    VeniceWriter<byte[], byte[], byte[]> mockMainWriter = mock(VeniceWriter.class);
    ComplexVeniceWriter<byte[], byte[], byte[]> mockChildWriter = mock(ComplexVeniceWriter.class);
    CompletableFuture<Void> childWriterFuture = new CompletableFuture<>();
    doReturn(CompletableFuture.completedFuture(null)).when(mockMainWriter)
        .put(any(), any(), anyInt(), any(), any(), anyLong(), eq(null));
    doReturn(childWriterFuture).when(mockChildWriter)
        .complexPut(any(), any(), anyInt(), any(), any(), eq(null), eq(null));
    doReturn(TEST_VIEW_TOPIC_NAME).when(mockChildWriter).getTopicName();
    childWriterFuture.completeExceptionally(new VeniceException("Expected exception"));
    ComplexVeniceWriter[] childWriters = new ComplexVeniceWriter[1];
    childWriters[0] = mockChildWriter;
    AbstractVeniceWriter<byte[], byte[], byte[]> compositeVeniceWriter =
        new CompositeVeniceWriter("test_v1", mockMainWriter, childWriters, null, defaultValueExtractor);
    CompletableFuture compositeWriteFuture = compositeVeniceWriter.put(new byte[1], new byte[1], 1, null);
    ExecutionException e = Assert.expectThrows(ExecutionException.class, compositeWriteFuture::get);
    Assert.assertTrue(e.getCause().getMessage().contains("Expected"));
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testFlush() {
    VeniceWriter<byte[], byte[], byte[]> mockMainWriter = mock(VeniceWriter.class);
    ComplexVeniceWriter<byte[], byte[], byte[]> mockChildWriter = mock(ComplexVeniceWriter.class);
    ComplexVeniceWriter[] childWriters = new ComplexVeniceWriter[1];
    childWriters[0] = mockChildWriter;
    AbstractVeniceWriter<byte[], byte[], byte[]> compositeVeniceWriter =
        new CompositeVeniceWriter("test_v1", mockMainWriter, childWriters, null, defaultValueExtractor);
    compositeVeniceWriter.flush();
    InOrder inOrder = inOrder(mockChildWriter, mockMainWriter);
    inOrder.verify(mockChildWriter).flush();
    inOrder.verify(mockMainWriter).flush();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testWritesAreInOrder() throws InterruptedException, ExecutionException {
    VeniceWriter<byte[], byte[], byte[]> mockMainWriter = mock(VeniceWriter.class);
    CompletableFuture<PubSubProduceResult> mainWriterFuture = CompletableFuture.completedFuture(null);
    PutMetadata mockPutMetadata = mock(PutMetadata.class);
    doReturn(mainWriterFuture).when(mockMainWriter)
        .put(any(), any(), anyInt(), eq(null), any(), anyLong(), eq(mockPutMetadata));
    PubSubProducerCallback childPubSubProducerCallback = mock(PubSubProducerCallback.class);
    DeleteMetadata deleteMetadata = mock(DeleteMetadata.class);
    doReturn(mainWriterFuture).when(mockMainWriter).delete(any(), eq(null), eq(deleteMetadata));
    ComplexVeniceWriter<byte[], byte[], byte[]> mockChildWriter = mock(ComplexVeniceWriter.class);
    CompletableFuture<PubSubProduceResult> childWriterFuture = CompletableFuture.completedFuture(null);
    doReturn(childWriterFuture).when(mockChildWriter)
        .complexPut(any(), any(), anyInt(), any(), any(), eq(childPubSubProducerCallback), any());
    doReturn(TEST_VIEW_TOPIC_NAME).when(mockChildWriter).getTopicName();
    ComplexVeniceWriter[] childWriters = new ComplexVeniceWriter[1];
    childWriters[0] = mockChildWriter;
    AbstractVeniceWriter<byte[], byte[], byte[]> compositeVeniceWriter =
        new CompositeVeniceWriter<byte[], byte[], byte[]>(
            "test_v1",
            mockMainWriter,
            childWriters,
            childPubSubProducerCallback,
            defaultValueExtractor);
    compositeVeniceWriter.put(new byte[1], new byte[1], 1, null, mockPutMetadata);
    CompletableFuture lastWriteFuture = compositeVeniceWriter.delete(new byte[1], null, deleteMetadata);
    lastWriteFuture.get();
    InOrder inOrder = inOrder(mockMainWriter, mockChildWriter);
    inOrder.verify(mockChildWriter)
        .complexPut(any(), any(), anyInt(), any(), any(), eq(childPubSubProducerCallback), any());
    inOrder.verify(mockMainWriter).put(any(), any(), anyInt(), eq(null), any(), anyLong(), eq(mockPutMetadata));
    inOrder.verify(mockMainWriter).delete(any(), eq(null), eq(deleteMetadata));
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testViewPartitionMapPassing() throws ExecutionException, InterruptedException {
    VeniceWriter<byte[], byte[], byte[]> mockMainWriter = mock(VeniceWriter.class);
    ComplexVeniceWriter<byte[], byte[], byte[]> mockChildWriter = mock(ComplexVeniceWriter.class);
    String viewName = "testView";
    doReturn(viewName).when(mockChildWriter).getViewName();
    CompletableFuture<PubSubProduceResult> mainFuture = new CompletableFuture<>();
    PutMetadata mockPutMetadata = mock(PutMetadata.class);
    doReturn(mainFuture).when(mockMainWriter)
        .put(any(), any(), anyInt(), eq(null), any(), anyLong(), eq(mockPutMetadata));
    CompletableFuture<Void> childFuture = new CompletableFuture<>();
    PubSubProducerCallback childPubSubProducerCallback = mock(PubSubProducerCallback.class);
    doReturn(childFuture).when(mockChildWriter)
        .complexPut(any(), any(), anyInt(), any(), any(), eq(childPubSubProducerCallback), any());
    ComplexVeniceWriter[] childWriters = new ComplexVeniceWriter[1];
    childWriters[0] = mockChildWriter;
    AbstractVeniceWriter<byte[], byte[], byte[]> compositeVeniceWriter =
        new CompositeVeniceWriter<byte[], byte[], byte[]>(
            "test_v1",
            mockMainWriter,
            childWriters,
            childPubSubProducerCallback,
            defaultValueExtractor);
    CompletableFuture<PubSubProduceResult> compositeFuture =
        compositeVeniceWriter.put(new byte[1], new byte[1], 1, null, mockPutMetadata);
    ArgumentCaptor<Consumer<int[]>> partitionConsumerCaptor = ArgumentCaptor.forClass(Consumer.class);
    verify(mockChildWriter, times(1)).complexPut(
        any(),
        any(),
        anyInt(),
        any(),
        partitionConsumerCaptor.capture(),
        eq(childPubSubProducerCallback),
        any());
    partitionConsumerCaptor.getValue().accept(new int[] { 0, 2 });
    childFuture.complete(null);
    ArgumentCaptor<LeaderMetadataWrapper> leaderMetadataWrapperCaptor =
        ArgumentCaptor.forClass(LeaderMetadataWrapper.class);
    verify(mockMainWriter, times(1))
        .put(any(), any(), anyInt(), any(), leaderMetadataWrapperCaptor.capture(), anyLong(), eq(mockPutMetadata));
    mainFuture.complete(null);
    compositeFuture.get();
    Map<String, Set<Integer>> viewPartitionMap = leaderMetadataWrapperCaptor.getValue().getViewPartitionMap();
    Assert.assertNotNull(viewPartitionMap);
    Assert.assertEquals(viewPartitionMap.size(), 1);
    Assert.assertEquals(viewPartitionMap.get(viewName), new HashSet<>(Arrays.asList(0, 2)));
  }
}
