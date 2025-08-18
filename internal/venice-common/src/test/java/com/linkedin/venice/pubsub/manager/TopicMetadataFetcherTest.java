package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.mockito.ArgumentMatchers.any;
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
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.manager.TopicMetadataFetcher.ValueAndExpiryTime;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.Time;
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
  public void testGetEndPositionsForTopic() {
    // test consumer::partitionFor --> (null, empty list)
    when(consumerMock.partitionsFor(pubSubTopic)).thenReturn(null).thenReturn(Collections.emptyList());
    for (int i = 0; i < 2; i++) {
      verify(consumerMock, times(i)).partitionsFor(pubSubTopic);
      Map<PubSubTopicPartition, PubSubPosition> res = topicMetadataFetcher.getEndPositionsForTopic(pubSubTopic);
      assertEquals(res, Collections.emptyMap());
      assertEquals(res.size(), 0);
      verify(consumerMock, times(i + 1)).partitionsFor(pubSubTopic);
    }

    // test consumer::partitionFor returns non-empty list
    PubSubTopicPartitionInfo tp0Info = new PubSubTopicPartitionInfo(pubSubTopic, 0, true);
    PubSubTopicPartitionInfo tp1Info = new PubSubTopicPartitionInfo(pubSubTopic, 1, true);
    List<PubSubTopicPartitionInfo> partitionInfo = Arrays.asList(tp0Info, tp1Info);
    Map<PubSubTopicPartition, PubSubPosition> offsetsMap = new HashMap<>(2);
    offsetsMap.put(tp0Info.getTopicPartition(), ApacheKafkaOffsetPosition.of(111L));
    offsetsMap.put(tp1Info.getTopicPartition(), ApacheKafkaOffsetPosition.of(222L));

    when(consumerMock.partitionsFor(pubSubTopic)).thenReturn(partitionInfo);
    when(consumerMock.endPositions(eq(offsetsMap.keySet()), any(Duration.class))).thenReturn(offsetsMap);

    Map<PubSubTopicPartition, PubSubPosition> res = topicMetadataFetcher.getEndPositionsForTopic(pubSubTopic);
    assertEquals(res.size(), offsetsMap.size());
    assertEquals(res.get(tp0Info.getTopicPartition()), ApacheKafkaOffsetPosition.of(111L));
    assertEquals(res.get(tp1Info.getTopicPartition()), ApacheKafkaOffsetPosition.of(222L));
    assertEquals(
        topicMetadataFetcher.getLatestOffsetCachedNonBlocking(new PubSubTopicPartitionImpl(pubSubTopic, 0)),
        PubSubConstants.UNKNOWN_LATEST_OFFSET);

    verify(consumerMock, times(3)).partitionsFor(pubSubTopic);
    verify(consumerMock, times(1)).endPositions(eq(offsetsMap.keySet()), any(Duration.class));

    // check if consumer was released back to the pool
    assertEquals(pubSubConsumerPool.size(), 1);
  }

  @Test
  public void testGetStartPositionsForTopic() {
    // test consumer::partitionFor --> (null, empty list)
    when(consumerMock.partitionsFor(pubSubTopic)).thenReturn(null).thenReturn(Collections.emptyList());
    for (int i = 0; i < 2; i++) {
      verify(consumerMock, times(i)).partitionsFor(pubSubTopic);
      Map<PubSubTopicPartition, PubSubPosition> res = topicMetadataFetcher.getStartPositionsForTopic(pubSubTopic);
      assertEquals(res, Collections.emptyMap());
      assertEquals(res.size(), 0);
      verify(consumerMock, times(i + 1)).partitionsFor(pubSubTopic);
    }

    // test consumer::partitionFor returns non-empty list
    PubSubTopicPartitionInfo tp0Info = new PubSubTopicPartitionInfo(pubSubTopic, 0, true);
    PubSubTopicPartitionInfo tp1Info = new PubSubTopicPartitionInfo(pubSubTopic, 1, true);
    List<PubSubTopicPartitionInfo> partitionInfo = Arrays.asList(tp0Info, tp1Info);

    // Create position objects once for reuse
    PubSubPosition tp0Position = ApacheKafkaOffsetPosition.of(0L);
    PubSubPosition tp1Position = ApacheKafkaOffsetPosition.of(10L);

    Map<PubSubTopicPartition, PubSubPosition> offsetsMap = new HashMap<>(2);
    offsetsMap.put(tp0Info.getTopicPartition(), tp0Position);
    offsetsMap.put(tp1Info.getTopicPartition(), tp1Position);

    when(consumerMock.partitionsFor(pubSubTopic)).thenReturn(partitionInfo);
    when(consumerMock.beginningPositions(eq(offsetsMap.keySet()), any(Duration.class))).thenReturn(offsetsMap);

    Map<PubSubTopicPartition, PubSubPosition> res = topicMetadataFetcher.getStartPositionsForTopic(pubSubTopic);
    assertEquals(res.size(), offsetsMap.size());
    assertEquals(res.get(tp0Info.getTopicPartition()), tp0Position);
    assertEquals(res.get(tp1Info.getTopicPartition()), tp1Position);

    verify(consumerMock, times(3)).partitionsFor(pubSubTopic);
    verify(consumerMock, times(1)).beginningPositions(eq(offsetsMap.keySet()), any(Duration.class));

    // check if consumer was released back to the pool
    assertEquals(pubSubConsumerPool.size(), 1);

    // mock validate topic
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);

    // Mock individual partition position calls
    Map<PubSubTopicPartition, PubSubPosition> tp0SingletonMap = new HashMap<>();
    tp0SingletonMap.put(tp0Info.getTopicPartition(), tp0Position);
    Map<PubSubTopicPartition, PubSubPosition> tp1SingletonMap = new HashMap<>();
    tp1SingletonMap.put(tp1Info.getTopicPartition(), tp1Position);

    when(consumerMock.beginningPositions(eq(Collections.singleton(tp0Info.getTopicPartition())), any(Duration.class)))
        .thenReturn(tp0SingletonMap);
    when(consumerMock.beginningPositions(eq(Collections.singleton(tp1Info.getTopicPartition())), any(Duration.class)))
        .thenReturn(tp1SingletonMap);

    assertEquals(topicMetadataFetcher.getStartPositionsForPartition(tp0Info.getTopicPartition()), tp0Position);
    assertEquals(topicMetadataFetcher.getStartPositionsForPartition(tp1Info.getTopicPartition()), tp1Position);
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
  public void testGetLatestOffset() {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    Map<PubSubTopicPartition, PubSubPosition> offsetMap = new HashMap<>();
    offsetMap.put(tp0, ApacheKafkaOffsetPosition.of(1001L));
    when(consumerMock.endPositions(eq(offsetMap.keySet()), any(Duration.class))).thenReturn(offsetMap);
    long latestOffset = topicMetadataFetcher.getLatestOffset(tp0);
    assertEquals(latestOffset, 1001L);

    // test when endOffsets returns null
    when(consumerMock.endPositions(eq(offsetMap.keySet()), any(Duration.class))).thenReturn(Collections.emptyMap());
    Throwable t = expectThrows(VeniceException.class, () -> topicMetadataFetcher.getLatestOffset(tp0));
    assertTrue(t.getMessage().contains("Got null position for:"), "Got: " + t.getMessage());
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

    Map<PubSubTopicPartition, PubSubPosition> offsetMap = new HashMap<>();
    offsetMap.put(tp0, ApacheKafkaOffsetPosition.of(1001L));
    when(consumerMock.endPositions(eq(offsetMap.keySet()), any(Duration.class))).thenReturn(offsetMap);
    long latestOffset = topicMetadataFetcher.getLatestOffsetWithRetries(tp0, 1);
    assertEquals(latestOffset, 1001L);

    // test when endOffsets returns null
    when(consumerMock.endPositions(eq(offsetMap.keySet()), any(Duration.class))).thenReturn(Collections.emptyMap());
    Throwable t = expectThrows(VeniceException.class, () -> topicMetadataFetcher.getLatestOffsetWithRetries(tp0, 1));
    assertTrue(t.getMessage().contains("Got null position for:"));
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
