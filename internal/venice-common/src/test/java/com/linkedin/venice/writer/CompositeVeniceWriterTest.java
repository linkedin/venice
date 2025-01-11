package com.linkedin.venice.writer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import java.util.concurrent.CompletableFuture;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CompositeVeniceWriterTest {
  @Test
  public void testFlushChecksForLastWriteFuture() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    CompletableFuture<PubSubProduceResult> mainWriterFuture = new CompletableFuture<>();
    doReturn(mainWriterFuture).when(mockWriter).put(any(), any(), anyInt(), eq(null));
    mainWriterFuture.completeExceptionally(new VeniceException("Expected exception"));
    AbstractVeniceWriter<byte[], byte[], byte[]> compositeVeniceWriter =
        new CompositeVeniceWriter("test_v1", mockWriter, new VeniceWriter[0], null);
    compositeVeniceWriter.put(new byte[1], new byte[1], 1, null);
    VeniceException e = Assert.expectThrows(VeniceException.class, compositeVeniceWriter::flush);
    Assert.assertTrue(e.getCause().getMessage().contains("Expected"));
  }

  @Test
  public void testWritesAreInOrder() throws InterruptedException {
    VeniceWriter<byte[], byte[], byte[]> mockMainWriter = mock(VeniceWriter.class);
    CompletableFuture<PubSubProduceResult> mainWriterFuture = CompletableFuture.completedFuture(null);
    doReturn(mainWriterFuture).when(mockMainWriter).put(any(), any(), anyInt(), eq(null));
    PubSubProducerCallback deletePubSubProducerCallback = mock(PubSubProducerCallback.class);
    DeleteMetadata deleteMetadata = mock(DeleteMetadata.class);
    doReturn(mainWriterFuture).when(mockMainWriter).delete(any(), eq(deletePubSubProducerCallback), eq(deleteMetadata));
    VeniceWriter<byte[], byte[], byte[]> mockChildWriter = mock(VeniceWriter.class);
    CompletableFuture<PubSubProduceResult> childWriterFuture = new CompletableFuture<>();
    doReturn(childWriterFuture).when(mockChildWriter).put(any(), any(), anyInt(), eq(null));
    doReturn(childWriterFuture).when(mockChildWriter)
        .delete(any(), eq(deletePubSubProducerCallback), eq(deleteMetadata));
    VeniceWriter[] childWriters = new VeniceWriter[1];
    childWriters[0] = mockChildWriter;
    AbstractVeniceWriter<byte[], byte[], byte[]> compositeVeniceWriter =
        new CompositeVeniceWriter<byte[], byte[], byte[]>("test_v1", mockMainWriter, childWriters, null);
    compositeVeniceWriter.put(new byte[1], new byte[1], 1, null);
    compositeVeniceWriter.delete(new byte[1], deletePubSubProducerCallback, deleteMetadata);
    verify(mockMainWriter, never()).put(any(), any(), anyInt(), eq(null));
    verify(mockMainWriter, never()).delete(any(), eq(deletePubSubProducerCallback), eq(deleteMetadata));
    Thread.sleep(1000);
    verify(mockMainWriter, never()).put(any(), any(), anyInt(), eq(null));
    verify(mockMainWriter, never()).delete(any(), eq(deletePubSubProducerCallback), eq(deleteMetadata));
    childWriterFuture.complete(null);
    verify(mockMainWriter, timeout(1000)).put(any(), any(), anyInt(), eq(null));
    verify(mockMainWriter, timeout(1000)).delete(any(), eq(deletePubSubProducerCallback), eq(deleteMetadata));
  }
}
