package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_POLLING_FOR_METADATA_RETRY_MAX_ATTEMPT;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_NO_PRODUCER_TIME_IN_EMPTY_TOPIC_PARTITION;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.manager.TopicMetadataFetcher.ValueAndExpiryTime;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.Time;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongMaps;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TopicMetadataFetcherTest {
  private TopicMetadataFetcher topicMetadataFetcher;
  private String pubSubClusterAddress = "venicedb.pubsub.standalone:9092";
  private PubSubAdminAdapter adminMock;
  private BlockingQueue<PubSubConsumerAdapter> pubSubConsumerPool;
  private ThreadPoolExecutor threadPoolExecutor;
  private long cachedEntryTtlInNs = TimeUnit.MINUTES.toNanos(5);
  private PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private String topicName = "testTopicName";
  private PubSubTopic pubSubTopic;
  private PubSubConsumerAdapter consumerMock;
  private TopicManagerStats stats;

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws InterruptedException {
    consumerMock = mock(PubSubConsumerAdapter.class);
    pubSubConsumerPool = new LinkedBlockingQueue<>(1);
    pubSubConsumerPool.put(consumerMock);
    threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
    adminMock = mock(PubSubAdminAdapter.class);
    pubSubTopic = pubSubTopicRepository.getTopic(topicName);
    stats = mock(TopicManagerStats.class);
    topicMetadataFetcher = new TopicMetadataFetcher(
        pubSubClusterAddress,
        stats,
        adminMock,
        pubSubConsumerPool,
        threadPoolExecutor,
        cachedEntryTtlInNs);
    assertEquals(pubSubConsumerPool.size(), 1);
  }

  @Test
  public void testClose() throws InterruptedException {
    CountDownLatch signalReceiver = new CountDownLatch(1);
    List<PubSubTopicPartitionInfo> partitions = new ArrayList<>();
    CountDownLatch holdConsumer = new CountDownLatch(1);
    doAnswer(invocation -> {
      try {
        signalReceiver.countDown();
        return partitions;
      } catch (Exception e) {
        throw e;
      } finally {
        holdConsumer.await();
      }
    }).when(consumerMock).partitionsFor(pubSubTopic);

    CompletableFuture<List<PubSubTopicPartitionInfo>> future = CompletableFuture
        .supplyAsync(() -> topicMetadataFetcher.getTopicPartitionInfo(pubSubTopic), threadPoolExecutor);

    if (!signalReceiver.await(3, TimeUnit.MINUTES)) {
      fail("Timed out waiting for signalReceiver");
    }

    try {
      topicMetadataFetcher.close();
    } catch (Exception e) {
      fail("TopicMetadataFetcher::close should not throw exception when closing");
    }
    Throwable t = expectThrows(ExecutionException.class, future::get);
    assertTrue(ExceptionUtils.recursiveClassEquals(t, InterruptedException.class));
    verify(consumerMock, times(1)).partitionsFor(pubSubTopic);
    verify(consumerMock, times(1)).close();
  }

  @Test
  public void testValidateTopicPartition() {
    assertThrows(NullPointerException.class, () -> topicMetadataFetcher.validateTopicPartition(null));

    final PubSubTopicPartition tp1 = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topicName), -1);
    assertThrows(IllegalArgumentException.class, () -> topicMetadataFetcher.validateTopicPartition(tp1));

    final PubSubTopicPartition tp2 = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topicName), 0);
    TopicMetadataFetcher topicMetadataFetcherSpy = spy(topicMetadataFetcher);
    doReturn(false).when(topicMetadataFetcherSpy).containsTopicCached(tp2.getPubSubTopic());
    Exception e =
        expectThrows(PubSubTopicDoesNotExistException.class, () -> topicMetadataFetcherSpy.validateTopicPartition(tp2));
    assertTrue(e.getMessage().contains("does not exist"));

    doReturn(true).when(topicMetadataFetcherSpy).containsTopicCached(tp2.getPubSubTopic());
    topicMetadataFetcherSpy.validateTopicPartition(tp2);

    verify(topicMetadataFetcherSpy, times(2)).containsTopicCached(tp2.getPubSubTopic());
  }

  @Test
  public void testContainsTopic() {
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(false);
    assertFalse(topicMetadataFetcher.containsTopic(pubSubTopic));

    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);
    assertTrue(topicMetadataFetcher.containsTopic(pubSubTopic));

    verify(adminMock, times(2)).containsTopic(pubSubTopic);
  }

  @Test
  public void testContainsTopicAsync() {
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(false);
    assertFalse(topicMetadataFetcher.containsTopicAsync(pubSubTopic).join());

    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);
    assertTrue(topicMetadataFetcher.containsTopicAsync(pubSubTopic).join());

    doThrow(new PubSubClientException("Test")).when(adminMock).containsTopic(pubSubTopic);
    CompletableFuture<Boolean> future = topicMetadataFetcher.containsTopicAsync(pubSubTopic);
    ExecutionException e = expectThrows(ExecutionException.class, future::get);
    assertTrue(ExceptionUtils.recursiveClassEquals(e, PubSubClientException.class));

    verify(adminMock, times(3)).containsTopic(pubSubTopic);
  }

  @Test
  public void testUpdateCacheAsyncWhenCachedValueIsNotStaleOrWhenUpdateIsInProgressShouldNotUpdateCache() {
    // Set the cached value to be not stale and update not in progress
    Supplier<CompletableFuture<Boolean>> cfSupplierMock = mock(Supplier.class);
    Map<PubSubTopic, ValueAndExpiryTime<Boolean>> cache = new ConcurrentHashMap<>();
    ValueAndExpiryTime<Boolean> cachedValue = topicMetadataFetcher.new ValueAndExpiryTime<>(true);
    cache.put(pubSubTopic, cachedValue);
    topicMetadataFetcher.updateCacheAsync(pubSubTopic, cachedValue, cache, cfSupplierMock);
    verify(cfSupplierMock, never()).get();

    // Set the cached value to be stale and update in progress
    cachedValue.setExpiryTimeNs(System.nanoTime() - 1);
    cachedValue.setUpdateInProgressStatus(true);
    topicMetadataFetcher.updateCacheAsync(pubSubTopic, cachedValue, cache, cfSupplierMock);
    verify(cfSupplierMock, never()).get();
  }

  @Test
  public void testUpdateCacheAsync() {
    // WhenCachedValueIsNull --> ShouldUpdateCache
    Supplier<CompletableFuture<Boolean>> cfSupplierMock = mock(Supplier.class);
    when(cfSupplierMock.get()).thenReturn(CompletableFuture.completedFuture(true));
    Map<PubSubTopic, ValueAndExpiryTime<Boolean>> cache = new ConcurrentHashMap<>();
    topicMetadataFetcher.updateCacheAsync(pubSubTopic, null, cache, cfSupplierMock);
    assertEquals(cache.size(), 1);
    assertTrue(cache.containsKey(pubSubTopic));
    // if we can acquire the lock, it means it was released after the update
    assertTrue(cache.get(pubSubTopic).tryAcquireUpdateLock());
    verify(cfSupplierMock, times(1)).get();

    // WhenCachedValueIsStaleAndWhenAsyncUpdateSucceeds --> ShouldUpdateCache
    ValueAndExpiryTime<Boolean> cachedValue = topicMetadataFetcher.new ValueAndExpiryTime<>(true);
    cachedValue.setExpiryTimeNs(System.nanoTime() - 1);
    topicMetadataFetcher.updateCacheAsync(pubSubTopic, cachedValue, cache, cfSupplierMock);
    assertEquals(cache.size(), 1);
    assertTrue(cache.containsKey(pubSubTopic));
    // if we can acquire the lock, it means it was released after the update
    assertTrue(cache.get(pubSubTopic).tryAcquireUpdateLock());
    assertTrue(cache.get(pubSubTopic).getValue());
    verify(cfSupplierMock, times(2)).get();

    // WhenAsyncUpdateFails --> ShouldRemoveFromCache
    cache.remove(pubSubTopic);
    CompletableFuture<Boolean> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new PubSubClientException("Test"));
    when(cfSupplierMock.get()).thenReturn(failedFuture);
    topicMetadataFetcher.updateCacheAsync(pubSubTopic, null, cache, cfSupplierMock);
    assertEquals(cache.size(), 0);
    assertFalse(cache.containsKey(pubSubTopic));
    verify(cfSupplierMock, times(3)).get();
  }

  @Test
  public void testGetTopicLatestOffsets() {
    // test consumer::partitionFor --> (null, empty list)
    when(consumerMock.partitionsFor(pubSubTopic)).thenReturn(null).thenReturn(Collections.emptyList());
    for (int i = 0; i < 2; i++) {
      verify(consumerMock, times(i)).partitionsFor(pubSubTopic);
      Int2LongMap res = topicMetadataFetcher.getTopicLatestOffsets(pubSubTopic);
      assertEquals(res, Int2LongMaps.EMPTY_MAP);
      assertEquals(res.size(), 0);
      verify(consumerMock, times(i + 1)).partitionsFor(pubSubTopic);
    }

    // test consumer::partitionFor returns non-empty list
    PubSubTopicPartitionInfo tp0Info = new PubSubTopicPartitionInfo(pubSubTopic, 0, true);
    PubSubTopicPartitionInfo tp1Info = new PubSubTopicPartitionInfo(pubSubTopic, 1, true);
    List<PubSubTopicPartitionInfo> partitionInfo = Arrays.asList(tp0Info, tp1Info);
    Map<PubSubTopicPartition, Long> offsetsMap = new ConcurrentHashMap<>();
    offsetsMap.put(tp0Info.getTopicPartition(), 111L);
    offsetsMap.put(tp1Info.getTopicPartition(), 222L);

    when(consumerMock.partitionsFor(pubSubTopic)).thenReturn(partitionInfo);
    when(consumerMock.endOffsets(eq(offsetsMap.keySet()), any(Duration.class))).thenReturn(offsetsMap);

    Int2LongMap res = topicMetadataFetcher.getTopicLatestOffsets(pubSubTopic);
    assertEquals(res.size(), offsetsMap.size());
    assertEquals(res.get(0), 111L);
    assertEquals(res.get(1), 222L);
    assertEquals(
        topicMetadataFetcher.getLatestOffsetCachedNonBlocking(new PubSubTopicPartitionImpl(pubSubTopic, 0)),
        PubSubConstants.UNKNOWN_LATEST_OFFSET);

    verify(consumerMock, times(3)).partitionsFor(pubSubTopic);
    verify(consumerMock, times(1)).endOffsets(eq(offsetsMap.keySet()), any(Duration.class));

    // check if consumer was released back to the pool
    assertEquals(pubSubConsumerPool.size(), 1);
  }

  @Test
  public void testGetTopicPartitionInfo() {
    PubSubTopicPartitionInfo tp0Info = new PubSubTopicPartitionInfo(pubSubTopic, 0, true);
    PubSubTopicPartitionInfo tp1Info = new PubSubTopicPartitionInfo(pubSubTopic, 1, true);
    List<PubSubTopicPartitionInfo> partitionInfo = Arrays.asList(tp0Info, tp1Info);
    when(consumerMock.partitionsFor(pubSubTopic)).thenReturn(partitionInfo);
    List<PubSubTopicPartitionInfo> res = topicMetadataFetcher.getTopicPartitionInfo(pubSubTopic);
    assertEquals(res.size(), partitionInfo.size());
    assertEquals(res.get(0), tp0Info);
    assertEquals(res.get(1), tp1Info);
    assertEquals(pubSubConsumerPool.size(), 1);
  }

  @Test
  public void testConsumeLatestRecords() {
    // test invalid input
    PubSubTopicPartition invalidTp = new PubSubTopicPartitionImpl(pubSubTopic, -1);
    Throwable t =
        expectThrows(IllegalArgumentException.class, () -> topicMetadataFetcher.consumeLatestRecords(invalidTp, 1));
    assertTrue(t.getMessage().contains("Invalid partition number"));

    // test invalid last record count
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    t = expectThrows(
        IllegalArgumentException.class,
        () -> topicMetadataFetcher.consumeLatestRecords(topicPartition, 0));
    assertTrue(t.getMessage().contains("Last record count must be greater than or equal to 1."));

    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);

    // test when endOffsets returns null
    when(consumerMock.endOffsets(eq(Collections.singleton(topicPartition)), any(Duration.class))).thenReturn(null);
    t = expectThrows(VeniceException.class, () -> topicMetadataFetcher.consumeLatestRecords(topicPartition, 1));
    assertTrue(t.getMessage().contains("Failed to get the end offset for topic-partition:"));

    // test when endOffsets returns empty map
    when(consumerMock.endOffsets(eq(Collections.singleton(topicPartition)), any(Duration.class)))
        .thenReturn(Collections.emptyMap());
    t = expectThrows(VeniceException.class, () -> topicMetadataFetcher.consumeLatestRecords(topicPartition, 1));
    assertTrue(t.getMessage().contains("Failed to get the end offset for topic-partition:"));

    // test when there are no records to consume as endOffset is 0
    Map<PubSubTopicPartition, Long> endOffsetsMap = new HashMap<>();
    endOffsetsMap.put(topicPartition, 0L);
    when(consumerMock.endOffsets(eq(Collections.singletonList(topicPartition)), any(Duration.class)))
        .thenReturn(endOffsetsMap);
    List<DefaultPubSubMessage> consumedRecords = topicMetadataFetcher.consumeLatestRecords(topicPartition, 1);
    assertEquals(consumedRecords.size(), 0);

    // test when beginningOffset (non-zero) is same as endOffset
    endOffsetsMap.put(topicPartition, 1L);
    when(consumerMock.endOffsets(eq(Collections.singletonList(topicPartition)), any(Duration.class)))
        .thenReturn(endOffsetsMap);
    when(consumerMock.beginningOffset(eq(topicPartition), any(Duration.class))).thenReturn(1L);
    consumedRecords = topicMetadataFetcher.consumeLatestRecords(topicPartition, 1);
    assertEquals(consumedRecords.size(), 0);

    long endOffset = 10;
    long startOffset = 3;
    int numRecordsToRead = 5; // read records at offsets 5, 6, 7, 8, 9
    long consumePastOffset = 4; // subscribe at offset
    endOffsetsMap.put(topicPartition, endOffset);
    when(consumerMock.endOffsets(eq(Collections.singletonList(topicPartition)), any(Duration.class)))
        .thenReturn(endOffsetsMap);
    when(consumerMock.beginningOffset(eq(topicPartition), any(Duration.class))).thenReturn(startOffset);
    verify(consumerMock, never()).subscribe(topicPartition, consumePastOffset);
    verify(consumerMock, never()).poll(anyLong());
    verify(consumerMock, never()).unSubscribe(eq(topicPartition));

    // test when poll returns no records
    when(consumerMock.poll(anyLong())).thenReturn(Collections.emptyMap());
    t = expectThrows(
        VeniceException.class,
        () -> topicMetadataFetcher.consumeLatestRecords(topicPartition, numRecordsToRead));
    assertNotNull(t.getMessage());
    assertTrue(t.getMessage().contains("Failed to get records from topic-partition:"));
    verify(consumerMock, times(1)).subscribe(topicPartition, consumePastOffset);
    verify(consumerMock, times(PUBSUB_CONSUMER_POLLING_FOR_METADATA_RETRY_MAX_ATTEMPT)).poll(anyLong());
    verify(consumerMock, times(1)).unSubscribe(eq(topicPartition));

    // poll returns 1, then 2 and then 4 records (to simulate a condition where records get added after getEndOffsets
    // API call)
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> batch1 = new HashMap<>();
    batch1.put(topicPartition, Collections.singletonList(getPubSubMessage(topicPartition, true, 5)));
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> batch2 = new HashMap<>();
    batch2.put(
        topicPartition,
        Arrays.asList(getPubSubMessage(topicPartition, true, 6), getPubSubMessage(topicPartition, false, 7)));
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> batch3 = new HashMap<>();
    batch3.put(
        topicPartition,
        Arrays.asList(
            getPubSubMessage(topicPartition, false, 8),
            getPubSubMessage(topicPartition, true, 9),
            getPubSubMessage(topicPartition, true, 10),
            getPubSubMessage(topicPartition, false, 11)));
    when(consumerMock.poll(anyLong())).thenReturn(batch1).thenReturn(batch2).thenReturn(batch3);

    List<DefaultPubSubMessage> allConsumedRecords =
        topicMetadataFetcher.consumeLatestRecords(topicPartition, numRecordsToRead);
    assertTrue(allConsumedRecords.size() >= numRecordsToRead);
    long firstOffset = allConsumedRecords.get(0).getPosition().getNumericOffset();
    assertEquals(firstOffset, 5);
    long lastOffset = allConsumedRecords.get(allConsumedRecords.size() - 1).getPosition().getNumericOffset();
    assertEquals(lastOffset, 11);
    verify(consumerMock, times(2)).subscribe(topicPartition, consumePastOffset);
    verify(consumerMock, times(6)).poll(anyLong()); // 3 from prev test and 3 from this test
    verify(consumerMock, times(2)).unSubscribe(eq(topicPartition));
  }

  private DefaultPubSubMessage getHeartBeatPubSubMessage(PubSubTopicPartition topicPartition, long offset) {
    KafkaKey key = KafkaKey.HEART_BEAT;
    KafkaMessageEnvelope val = mock(KafkaMessageEnvelope.class);
    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.setMessageTimestamp(System.nanoTime());
    when(val.getProducerMetadata()).thenReturn(producerMetadata);
    return new ImmutablePubSubMessage(
        key,
        val,
        topicPartition,
        ApacheKafkaOffsetPosition.of(offset),
        System.currentTimeMillis(),
        512);
  }

  private DefaultPubSubMessage getPubSubMessage(
      PubSubTopicPartition topicPartition,
      boolean isControlMessage,
      long offset) {
    KafkaKey key = mock(KafkaKey.class);
    when(key.isControlMessage()).thenReturn(isControlMessage);
    KafkaMessageEnvelope val = mock(KafkaMessageEnvelope.class);
    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.setMessageTimestamp(System.nanoTime());
    when(val.getProducerMetadata()).thenReturn(producerMetadata);
    return new ImmutablePubSubMessage(
        key,
        val,
        topicPartition,
        ApacheKafkaOffsetPosition.of(offset),
        System.currentTimeMillis(),
        512);
  }

  @Test
  public void testGetProducerTimestampOfLastDataMessage() {
    // test when there are no records to consume
    TopicMetadataFetcher metadataFetcherSpy = spy(topicMetadataFetcher);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    doReturn(Collections.emptyList()).when(metadataFetcherSpy).consumeLatestRecords(eq(topicPartition), anyInt());
    long timestamp = metadataFetcherSpy.getProducerTimestampOfLastDataMessage(topicPartition);
    assertEquals(timestamp, PUBSUB_NO_PRODUCER_TIME_IN_EMPTY_TOPIC_PARTITION);
    verify(metadataFetcherSpy, times(1)).consumeLatestRecords(eq(topicPartition), anyInt());

    // test when there are no data messages and heartbeat messages to consume
    DefaultPubSubMessage cm = getPubSubMessage(topicPartition, true, 5);
    doReturn(Collections.singletonList(cm)).when(metadataFetcherSpy).consumeLatestRecords(eq(topicPartition), anyInt());
    Throwable t = expectThrows(
        VeniceException.class,
        () -> metadataFetcherSpy.getProducerTimestampOfLastDataMessage(topicPartition));
    assertTrue(t.getMessage().contains("No data message found in topic-partition"));

    // test when there are heartbeat messages but no data messages to consume
    DefaultPubSubMessage hm = getHeartBeatPubSubMessage(topicPartition, 6);
    doReturn(Collections.singletonList(hm)).when(metadataFetcherSpy).consumeLatestRecords(eq(topicPartition), anyInt());
    timestamp = metadataFetcherSpy.getProducerTimestampOfLastDataMessage(topicPartition);
    assertEquals(timestamp, hm.getValue().getProducerMetadata().getMessageTimestamp());

    // test when there are data messages to consume
    DefaultPubSubMessage dm0 = getPubSubMessage(topicPartition, false, 4);
    doReturn(Collections.singletonList(dm0)).when(metadataFetcherSpy)
        .consumeLatestRecords(eq(topicPartition), anyInt());
    timestamp = metadataFetcherSpy.getProducerTimestampOfLastDataMessage(topicPartition);
    assertEquals(timestamp, dm0.getValue().getProducerMetadata().getMessageTimestamp());

    // test: first return one control message and then one data message
    doReturn(Collections.singletonList(cm)).doReturn(Collections.singletonList(dm0))
        .when(metadataFetcherSpy)
        .consumeLatestRecords(eq(topicPartition), anyInt());
    timestamp = metadataFetcherSpy.getProducerTimestampOfLastDataMessage(topicPartition);
    assertEquals(timestamp, dm0.getValue().getProducerMetadata().getMessageTimestamp());

    // test: return 2 data messages
    DefaultPubSubMessage dm1 = getPubSubMessage(topicPartition, false, 3);
    doReturn(Arrays.asList(dm1, dm0)).when(metadataFetcherSpy).consumeLatestRecords(eq(topicPartition), anyInt());
    timestamp = metadataFetcherSpy.getProducerTimestampOfLastDataMessage(topicPartition);
    assertEquals(timestamp, dm0.getValue().getProducerMetadata().getMessageTimestamp());
  }

  @Test
  public void testGetLatestOffset() {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    Map<PubSubTopicPartition, Long> offsetMap = new HashMap<>();
    offsetMap.put(tp0, 1001L);
    when(consumerMock.endOffsets(eq(offsetMap.keySet()), any(Duration.class))).thenReturn(offsetMap);
    long latestOffset = topicMetadataFetcher.getLatestOffset(tp0);
    assertEquals(latestOffset, 1001L);

    // test when endOffsets returns null
    when(consumerMock.endOffsets(eq(offsetMap.keySet()), any(Duration.class))).thenReturn(Collections.emptyMap());
    Throwable t = expectThrows(VeniceException.class, () -> topicMetadataFetcher.getLatestOffset(tp0));
    assertTrue(t.getMessage().contains("Got null as latest offset for"));
    assertEquals(pubSubConsumerPool.size(), 1);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testGetLatestOffsetWithRetries() throws ExecutionException, InterruptedException {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);

    TopicMetadataFetcher topicMetadataFetcherSpy = spy(topicMetadataFetcher);
    doThrow(new PubSubTopicDoesNotExistException("Test1")).doThrow(new PubSubOpTimeoutException("Test2"))
        .doThrow(new PubSubOpTimeoutException("Test3"))
        .doReturn(99L)
        .when(topicMetadataFetcherSpy)
        .getLatestOffset(tp0);
    assertEquals((long) topicMetadataFetcherSpy.getLatestOffsetWithRetriesAsync(tp0, 5).get(), 99L);
    verify(topicMetadataFetcherSpy, times(4)).getLatestOffset(tp0);

    doThrow(new PubSubTopicDoesNotExistException("Test1")).when(topicMetadataFetcherSpy).getLatestOffset(tp0);
    expectThrows(
        PubSubTopicDoesNotExistException.class,
        () -> topicMetadataFetcherSpy.getLatestOffsetWithRetries(tp0, 1));

    Map<PubSubTopicPartition, Long> offsetMap = new HashMap<>();
    offsetMap.put(tp0, 1001L);
    when(consumerMock.endOffsets(eq(offsetMap.keySet()), any(Duration.class))).thenReturn(offsetMap);
    long latestOffset = topicMetadataFetcher.getLatestOffsetWithRetries(tp0, 1);
    assertEquals(latestOffset, 1001L);

    // test when endOffsets returns null
    when(consumerMock.endOffsets(eq(offsetMap.keySet()), any(Duration.class))).thenReturn(Collections.emptyMap());
    Throwable t = expectThrows(VeniceException.class, () -> topicMetadataFetcher.getLatestOffsetWithRetries(tp0, 1));
    assertTrue(t.getMessage().contains("Got null as latest offset for"));
    assertEquals(pubSubConsumerPool.size(), 1);
  }

  @Test
  public void testGetOffsetForTime() {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);
    long latestOffset = 12321L;
    TopicMetadataFetcher topicMetadataFetcherSpy = spy(topicMetadataFetcher);
    doReturn(latestOffset).when(topicMetadataFetcherSpy).getLatestOffset(tp0);

    long ts = System.currentTimeMillis();
    when(consumerMock.offsetForTime(eq(tp0), eq(ts), any(Duration.class))).thenReturn(9988L);
    assertEquals(topicMetadataFetcherSpy.getOffsetForTime(tp0, ts), 9988L);
    assertEquals(pubSubConsumerPool.size(), 1);

    // test when offsetForTime returns null
    when(consumerMock.offsetForTime(eq(tp0), eq(ts), any(Duration.class))).thenReturn(null);
    assertEquals(topicMetadataFetcherSpy.getOffsetForTime(tp0, ts), latestOffset);
    assertEquals(pubSubConsumerPool.size(), 1);
  }

  @Test
  public void testConsumerIsReleasedBackToPoolWhenThreadIsTerminated() {
    CountDownLatch signalReceiver = new CountDownLatch(1);
    assertEquals(pubSubConsumerPool.size(), 1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future future = executorService.submit(() -> {
      PubSubConsumerAdapter consumerAdapter = topicMetadataFetcher.acquireConsumer();
      try {
        signalReceiver.countDown();
        Thread.sleep(Integer.MAX_VALUE);
        System.out.println(pubSubConsumerPool.size());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new VeniceException("Thread interrupted");
      } finally {
        topicMetadataFetcher.releaseConsumer(consumerAdapter);
      }
    });

    try {
      signalReceiver.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    assertEquals(pubSubConsumerPool.size(), 0);
    future.cancel(true);
    waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      assertEquals(pubSubConsumerPool.size(), 1);
    });
  }
}
