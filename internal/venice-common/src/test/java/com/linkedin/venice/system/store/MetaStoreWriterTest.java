package com.linkedin.venice.system.store;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.SystemStore;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.Utils;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class MetaStoreWriterTest {
  Store metaStore = mock(Store.class);
  String metaStoreName = "testStore";

  @BeforeClass
  public void setUp() {
    doReturn(metaStoreName).when(metaStore).getName();
  }

  @Test
  public void testGetOrCreateMetaStoreWriterWithSystemStore() {
    // Setup mocks
    TopicManager topicManager = mock(TopicManager.class);
    VeniceWriterFactory writerFactory = mock(VeniceWriterFactory.class);
    HelixReadOnlyZKSharedSchemaRepository schemaRepo = mock(HelixReadOnlyZKSharedSchemaRepository.class);
    PubSubTopicRepository pubSubTopicRepository = mock(PubSubTopicRepository.class);
    Schema derivedComputeSchema = mock(Schema.class);

    // Mock SystemStore and its components
    SystemStore systemStore = mock(SystemStore.class);
    Store veniceStore = mock(Store.class);
    String systemStoreName = "venice_system_store_davinci_push_status_store_testStore";
    int expectedRTVersion = 5;

    when(systemStore.getName()).thenReturn(systemStoreName);
    when(systemStore.isSystemStore()).thenReturn(true);
    when(systemStore.getVeniceStore()).thenReturn(veniceStore);
    when(veniceStore.getLargestUsedRTVersionNumber()).thenReturn(expectedRTVersion);

    // Mock the PubSubTopic and TopicManager
    PubSubTopic mockTopic = mock(PubSubTopic.class);
    when(mockTopic.getName()).thenReturn(systemStoreName + "_rt_v" + expectedRTVersion);
    when(pubSubTopicRepository.getTopic(anyString())).thenReturn(mockTopic);
    when(topicManager.containsTopicAndAllPartitionsAreOnline(any())).thenReturn(true);

    // Mock VeniceWriter
    VeniceWriter mockWriter = mock(VeniceWriter.class);
    when(writerFactory.createVeniceWriter(any())).thenReturn(mockWriter);

    // Create MetaStoreWriter
    MetaStoreWriter metaStoreWriter = new MetaStoreWriter(
        topicManager,
        writerFactory,
        schemaRepo,
        derivedComputeSchema,
        pubSubTopicRepository,
        5000L,
        2,
        storeName -> Utils.getRealTimeTopicNameForSystemStore(systemStore));

    Assert.assertNotNull(metaStoreWriter.getOrCreateMetaStoreWriter(systemStoreName));
    verify(systemStore, times(1)).getVeniceStore();
  }

  @Test
  public void testGetOrCreateMetaStoreWriterWithRegularStore() {
    // Setup mocks
    TopicManager topicManager = mock(TopicManager.class);
    VeniceWriterFactory writerFactory = mock(VeniceWriterFactory.class);
    HelixReadOnlyZKSharedSchemaRepository schemaRepo = mock(HelixReadOnlyZKSharedSchemaRepository.class);
    PubSubTopicRepository pubSubTopicRepository = mock(PubSubTopicRepository.class);
    Schema derivedComputeSchema = mock(Schema.class);

    // Mock regular Store
    Store regularStore = mock(Store.class);
    String storeName = "regularStore";
    int expectedRTVersion = 3;

    when(regularStore.getName()).thenReturn(storeName);
    when(regularStore.isSystemStore()).thenReturn(false);
    when(regularStore.getLargestUsedRTVersionNumber()).thenReturn(expectedRTVersion);

    // Mock the PubSubTopic and TopicManager
    PubSubTopic mockTopic = mock(PubSubTopic.class);
    when(mockTopic.getName()).thenReturn(storeName + "_rt_v" + expectedRTVersion);
    when(pubSubTopicRepository.getTopic(anyString())).thenReturn(mockTopic);
    when(topicManager.containsTopicAndAllPartitionsAreOnline(any())).thenReturn(true);

    // Mock VeniceWriter
    VeniceWriter mockWriter = mock(VeniceWriter.class);
    when(writerFactory.createVeniceWriter(any())).thenReturn(mockWriter);

    // Create MetaStoreWriter
    MetaStoreWriter metaStoreWriter = new MetaStoreWriter(
        topicManager,
        writerFactory,
        schemaRepo,
        derivedComputeSchema,
        pubSubTopicRepository,
        5000L,
        2,
        storeName1 -> Utils.getRealTimeTopicNameForSystemStore(regularStore));

    Assert.assertNotNull(metaStoreWriter.getOrCreateMetaStoreWriter(storeName));
  }

  @Test
  public void testGetOrCreateMetaStoreWriterWithNonSystemStoreType() {
    // Setup mocks
    TopicManager topicManager = mock(TopicManager.class);
    VeniceWriterFactory writerFactory = mock(VeniceWriterFactory.class);
    HelixReadOnlyZKSharedSchemaRepository schemaRepo = mock(HelixReadOnlyZKSharedSchemaRepository.class);
    PubSubTopicRepository pubSubTopicRepository = mock(PubSubTopicRepository.class);
    Schema derivedComputeSchema = mock(Schema.class);

    // Mock a store that returns null for VeniceSystemStoreType but is a system store
    Store store = mock(Store.class);
    String storeName = "someSystemStore";
    int expectedRTVersion = 2;

    when(store.getName()).thenReturn(storeName);
    when(store.isSystemStore()).thenReturn(false); // type is null, so condition fails
    when(store.getLargestUsedRTVersionNumber()).thenReturn(expectedRTVersion);

    // Mock the PubSubTopic and TopicManager
    PubSubTopic mockTopic = mock(PubSubTopic.class);
    when(mockTopic.getName()).thenReturn(storeName + "_rt");
    when(pubSubTopicRepository.getTopic(anyString())).thenReturn(mockTopic);
    when(topicManager.containsTopicAndAllPartitionsAreOnline(any())).thenReturn(true);

    // Mock VeniceWriter
    VeniceWriter mockWriter = mock(VeniceWriter.class);
    when(writerFactory.createVeniceWriter(any())).thenReturn(mockWriter);

    // Create MetaStoreWriter
    MetaStoreWriter metaStoreWriter = new MetaStoreWriter(
        topicManager,
        writerFactory,
        schemaRepo,
        derivedComputeSchema,
        pubSubTopicRepository,
        5000L,
        2,
        storeName1 -> Utils.getRealTimeTopicNameForSystemStore(store));

    Assert.assertNotNull(metaStoreWriter.getOrCreateMetaStoreWriter(storeName));
  }

  @Test
  public void testMetaStoreWriterWillRestartUponProduceFailure() {
    MetaStoreWriter metaStoreWriter = mock(MetaStoreWriter.class);
    HelixReadOnlyZKSharedSchemaRepository schemaRepo = mock(HelixReadOnlyZKSharedSchemaRepository.class);
    GeneratedSchemaID generatedSchemaID = mock(GeneratedSchemaID.class);
    doReturn(true).when(generatedSchemaID).isValid();
    doReturn(generatedSchemaID).when(schemaRepo).getDerivedSchemaId(any(), any());
    doReturn(schemaRepo).when(metaStoreWriter).getSchemaRepository();
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
    metaStoreWriter.writeInUseValueSchema(anyString(), anyInt(), anyInt());
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
        numOfConcurrentVwCloseOps,
        storeName -> metaStoreName + "_rt");
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
