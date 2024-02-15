package com.linkedin.venice.system.store;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.VeniceResourceCloseResult;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.avro.Schema;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class MetaStoreWriterTest {
  @Test
  public void testMetaStoreWriterWillRestartUponProduceFailure() {
    MetaStoreWriter metaStoreWriter = mock(MetaStoreWriter.class);
    String metaStoreName = "testStore";
    ReentrantLock reentrantLock = new ReentrantLock();
    when(metaStoreWriter.getOrCreateMetaStoreWriterLock(metaStoreName)).thenReturn(reentrantLock);
    VeniceWriter badWriter = mock(VeniceWriter.class);
    when(badWriter.delete(any(), any())).thenThrow(new VeniceException("Bad producer call"));
    VeniceWriter goodWriter = mock(VeniceWriter.class);
    when(metaStoreWriter.getOrCreateMetaStoreWriter(metaStoreName)).thenReturn(badWriter, goodWriter);
    doCallRealMethod().when(metaStoreWriter).removeMetaStoreWriter(anyString());
    doCallRealMethod().when(metaStoreWriter).writeMessageWithRetry(anyString(), any());
    metaStoreWriter.writeMessageWithRetry(metaStoreName, vw -> vw.delete("a", null));
    verify(badWriter, times(1)).delete(any(), any());
    verify(goodWriter, times(1)).delete(any(), any());
  }

  @Test
  public void testMetaStoreWriterSendHeartbeatMessage() {
    // Mock
    MetaStoreWriter metaStoreWriter = mock(MetaStoreWriter.class);
    ReentrantLock reentrantLock = new ReentrantLock();
    String metaStoreName = "testStore";
    when(metaStoreWriter.getOrCreateMetaStoreWriterLock(anyString())).thenReturn(reentrantLock);
    VeniceWriter goodWriter = mock(VeniceWriter.class);
    when(metaStoreWriter.getOrCreateMetaStoreWriter(anyString())).thenReturn(goodWriter);
    doCallRealMethod().when(metaStoreWriter).writeHeartbeat(anyString(), anyLong());
    doCallRealMethod().when(metaStoreWriter).writeMessageWithRetry(anyString(), any());
    long timestamp = 123L;

    // Action
    metaStoreWriter.writeHeartbeat(metaStoreName, timestamp);
    ArgumentCaptor<StoreMetaKey> keyArgumentCaptor = ArgumentCaptor.forClass(StoreMetaKey.class);
    ArgumentCaptor<StoreMetaValue> valueArgumentCaptor = ArgumentCaptor.forClass(StoreMetaValue.class);
    ArgumentCaptor<Integer> schemaArgumentCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(goodWriter).put(keyArgumentCaptor.capture(), valueArgumentCaptor.capture(), schemaArgumentCaptor.capture());

    // Assertion
    StoreMetaKey capturedKey = keyArgumentCaptor.getValue();
    Assert.assertEquals(capturedKey.keyStrings, Collections.singletonList(metaStoreName));
    Assert.assertEquals(capturedKey.metadataType, MetaStoreDataType.HEARTBEAT.getValue());
    StoreMetaValue capturedValue = valueArgumentCaptor.getValue();
    Assert.assertEquals(capturedValue.timestamp, timestamp);
  }

  @DataProvider
  public Object[][] testCloseDataProvider() {
    return new Object[][] { { 5000, 30 }, { 4000, 2 }, { 3000, 11 }, { 2000, 0 } };
  }

  @Test(dataProvider = "testCloseDataProvider")
  public void testClose(long closeTimeoutMs, int numOfConcurrentVwCloseOps)
      throws IOException, ExecutionException, InterruptedException {
    TopicManager topicManager = mock(TopicManager.class);
    VeniceWriterFactory writerFactory = mock(VeniceWriterFactory.class);
    HelixReadOnlyZKSharedSchemaRepository schemaRepo = mock(HelixReadOnlyZKSharedSchemaRepository.class);
    PubSubTopicRepository pubSubTopicRepository = mock(PubSubTopicRepository.class);
    Schema derivedComputeSchema = mock(Schema.class);

    MetaStoreWriter metaStoreWriter = new MetaStoreWriter(
        topicManager,
        writerFactory,
        schemaRepo,
        derivedComputeSchema,
        pubSubTopicRepository,
        closeTimeoutMs,
        numOfConcurrentVwCloseOps);
    Map<String, VeniceWriter> metaStoreWriters = metaStoreWriter.getMetaStoreWriterMap();

    List<CompletableFuture<VeniceResourceCloseResult>> completedFutures = new ArrayList<>(20);
    for (int i = 0; i < 20; i++) {
      VeniceWriter veniceWriter = mock(VeniceWriter.class);
      metaStoreWriters.put("topic_" + i, veniceWriter);
      CompletableFuture<VeniceResourceCloseResult> future = mock(CompletableFuture.class);
      when(future.isDone()).thenReturn(true);
      if (i % 2 == 0) {
        when(future.get()).thenReturn(VeniceResourceCloseResult.SUCCESS);
      } else {
        when(future.get()).thenThrow(new ExecutionException(new VeniceException("Failed to close topic_" + i)));
      }
      when(veniceWriter.closeAsync(true)).thenReturn(future);
      when(veniceWriter.closeAsync(true)).thenReturn(future);
      completedFutures.add(future);
    }

    List<CompletableFuture<VeniceResourceCloseResult>> incompleteFutures = new ArrayList<>(20);
    for (int i = 20; i < 30; i++) {
      VeniceWriter veniceWriter = mock(VeniceWriter.class);
      metaStoreWriters.put("failed_topic_" + i, veniceWriter);
      CompletableFuture<VeniceResourceCloseResult> future = spy(new CompletableFuture<>());
      when(veniceWriter.closeAsync(true)).thenReturn(future);
      incompleteFutures.add(future);
    }

    long startTime = System.currentTimeMillis();
    metaStoreWriter.close();
    long elapsedTime = System.currentTimeMillis() - startTime;

    for (CompletableFuture<VeniceResourceCloseResult> future: completedFutures) {
      verify(future).isDone();
      verify(future).get();
    }

    for (CompletableFuture<VeniceResourceCloseResult> future: incompleteFutures) {
      verify(future).isDone();
      verify(future, never()).get();
    }

    // verify that elapsed time is close to closeTimeoutMs
    assertTrue(elapsedTime < (closeTimeoutMs + 5000L));
  }
}
