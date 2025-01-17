package com.linkedin.venice.writer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.mockito.InOrder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CompositeVeniceWriterTest {
  @Test
  public void testChildWriteExceptions() {
    VeniceWriter<byte[], byte[], byte[]> mockMainWriter = mock(VeniceWriter.class);
    VeniceWriter<byte[], byte[], byte[]> mockChildWriter = mock(VeniceWriter.class);
    CompletableFuture<PubSubProduceResult> childWriterFuture = new CompletableFuture<>();
    doReturn(CompletableFuture.completedFuture(null)).when(mockMainWriter).put(any(), any(), anyInt(), eq(null));
    doReturn(childWriterFuture).when(mockChildWriter).put(any(), any(), anyInt(), eq(null));
    childWriterFuture.completeExceptionally(new VeniceException("Expected exception"));
    VeniceWriter[] childWriters = new VeniceWriter[1];
    childWriters[0] = mockChildWriter;
    AbstractVeniceWriter<byte[], byte[], byte[]> compositeVeniceWriter =
        new CompositeVeniceWriter("test_v1", mockMainWriter, childWriters, null);
    CompletableFuture compositeWriteFuture = compositeVeniceWriter.put(new byte[1], new byte[1], 1, null);
    ExecutionException e = Assert.expectThrows(ExecutionException.class, compositeWriteFuture::get);
    Assert.assertTrue(e.getCause().getMessage().contains("Expected"));
  }

  @Test
  public void testFlush() {
    VeniceWriter<byte[], byte[], byte[]> mockMainWriter = mock(VeniceWriter.class);
    VeniceWriter<byte[], byte[], byte[]> mockChildWriter = mock(VeniceWriter.class);
    VeniceWriter[] childWriters = new VeniceWriter[1];
    childWriters[0] = mockChildWriter;
    AbstractVeniceWriter<byte[], byte[], byte[]> compositeVeniceWriter =
        new CompositeVeniceWriter("test_v1", mockMainWriter, childWriters, null);
    compositeVeniceWriter.flush();
    InOrder inOrder = inOrder(mockChildWriter, mockMainWriter);
    inOrder.verify(mockChildWriter).flush();
    inOrder.verify(mockMainWriter).flush();
  }

  @Test
  public void testWritesAreInOrder() throws InterruptedException, ExecutionException {
    VeniceWriter<byte[], byte[], byte[]> mockMainWriter = mock(VeniceWriter.class);
    CompletableFuture<PubSubProduceResult> mainWriterFuture = CompletableFuture.completedFuture(null);
    doReturn(mainWriterFuture).when(mockMainWriter).put(any(), any(), anyInt(), eq(null));
    PubSubProducerCallback childPubSubProducerCallback = mock(PubSubProducerCallback.class);
    DeleteMetadata deleteMetadata = mock(DeleteMetadata.class);
    doReturn(mainWriterFuture).when(mockMainWriter).delete(any(), eq(null), eq(deleteMetadata));
    VeniceWriter<byte[], byte[], byte[]> mockChildWriter = mock(VeniceWriter.class);
    CompletableFuture<PubSubProduceResult> childWriterFuture = CompletableFuture.completedFuture(null);
    doReturn(childWriterFuture).when(mockChildWriter).put(any(), any(), anyInt(), eq(childPubSubProducerCallback));
    doReturn(childWriterFuture).when(mockChildWriter)
        .delete(any(), eq(childPubSubProducerCallback), eq(deleteMetadata));
    VeniceWriter[] childWriters = new VeniceWriter[1];
    childWriters[0] = mockChildWriter;
    AbstractVeniceWriter<byte[], byte[], byte[]> compositeVeniceWriter =
        new CompositeVeniceWriter<byte[], byte[], byte[]>(
            "test_v1",
            mockMainWriter,
            childWriters,
            childPubSubProducerCallback);
    compositeVeniceWriter.put(new byte[1], new byte[1], 1, null);
    CompletableFuture lastWriteFuture = compositeVeniceWriter.delete(new byte[1], null, deleteMetadata);
    lastWriteFuture.get();
    InOrder inOrder = inOrder(mockMainWriter, mockChildWriter);
    inOrder.verify(mockMainWriter).put(any(), any(), anyInt(), eq(null));
    inOrder.verify(mockChildWriter).put(any(), any(), anyInt(), eq(childPubSubProducerCallback));
    inOrder.verify(mockMainWriter).delete(any(), eq(null), eq(deleteMetadata));
    inOrder.verify(mockChildWriter).delete(any(), eq(childPubSubProducerCallback), eq(deleteMetadata));
  }
}
