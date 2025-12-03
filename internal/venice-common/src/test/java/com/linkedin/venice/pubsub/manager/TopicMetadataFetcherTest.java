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
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
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
        topicMetadataFetcher.getLatestPositionCachedNonBlocking(new PubSubTopicPartitionImpl(pubSubTopic, 0)),
        PubSubSymbolicPosition.LATEST);

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
  public void testGetLatestPosition() {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    ApacheKafkaOffsetPosition p1001 = ApacheKafkaOffsetPosition.of(1001L);
    Map<PubSubTopicPartition, PubSubPosition> offsetMap = new HashMap<>();
    offsetMap.put(tp0, p1001);
    when(consumerMock.endPositions(eq(offsetMap.keySet()), any(Duration.class))).thenReturn(offsetMap);
    assertEquals(topicMetadataFetcher.getEndPositionForPartition(tp0), p1001);

    // test when endOffsets returns null
    when(consumerMock.endPositions(eq(offsetMap.keySet()), any(Duration.class))).thenReturn(Collections.emptyMap());
    Throwable t = expectThrows(VeniceException.class, () -> topicMetadataFetcher.getEndPositionForPartition(tp0));
    assertTrue(t.getMessage().contains("Got null position for:"), "Got: " + t.getMessage());
    assertEquals(pubSubConsumerPool.size(), 1);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testGetLatestPositionWithRetries() throws ExecutionException, InterruptedException {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);

    ApacheKafkaOffsetPosition p99 = ApacheKafkaOffsetPosition.of(99L);
    TopicMetadataFetcher topicMetadataFetcherSpy = spy(topicMetadataFetcher);
    TopicMetadataFetcher topicMetadataFetcher3 =
        doThrow(new PubSubTopicDoesNotExistException("Test1")).doThrow(new PubSubOpTimeoutException("Test2"))
            .doThrow(new PubSubOpTimeoutException("Test3"))
            .doReturn(p99)
            .when(topicMetadataFetcherSpy);
    topicMetadataFetcher3.getEndPositionForPartition(tp0);
    assertEquals(topicMetadataFetcherSpy.getLatestOffsetWithRetriesAsync(tp0, 5).get(), p99);
    TopicMetadataFetcher topicMetadataFetcher2 = verify(topicMetadataFetcherSpy, times(4));
    topicMetadataFetcher2.getEndPositionForPartition(tp0);

    TopicMetadataFetcher topicMetadataFetcher1 =
        doThrow(new PubSubTopicDoesNotExistException("Test1")).when(topicMetadataFetcherSpy);
    topicMetadataFetcher1.getEndPositionForPartition(tp0);
    expectThrows(
        PubSubTopicDoesNotExistException.class,
        () -> topicMetadataFetcherSpy.getLatestPositionWithRetries(tp0, 1));

    ApacheKafkaOffsetPosition p1001 = ApacheKafkaOffsetPosition.of(1001L);
    Map<PubSubTopicPartition, PubSubPosition> offsetMap = new HashMap<>();
    offsetMap.put(tp0, p1001);
    when(consumerMock.endPositions(eq(offsetMap.keySet()), any(Duration.class))).thenReturn(offsetMap);
    assertEquals(topicMetadataFetcher.getLatestPositionWithRetries(tp0, 1), p1001);

    // test when endOffsets returns null
    when(consumerMock.endPositions(eq(offsetMap.keySet()), any(Duration.class))).thenReturn(Collections.emptyMap());
    Throwable t = expectThrows(VeniceException.class, () -> topicMetadataFetcher.getLatestPositionWithRetries(tp0, 1));
    assertTrue(t.getMessage().contains("Got null position for:"));
    assertEquals(pubSubConsumerPool.size(), 1);
  }

  @Test
  public void testGetPositionForTime() {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);
    TopicMetadataFetcher topicMetadataFetcherSpy = spy(topicMetadataFetcher);
    ApacheKafkaOffsetPosition endPosition = ApacheKafkaOffsetPosition.of(1234L);
    doReturn(endPosition).when(topicMetadataFetcherSpy).getEndPositionForPartition(tp0);

    long ts = System.currentTimeMillis();
    ApacheKafkaOffsetPosition p9988 = ApacheKafkaOffsetPosition.of(9988L);
    when(consumerMock.getPositionByTimestamp(eq(tp0), eq(ts), any(Duration.class))).thenReturn(p9988);
    assertEquals(topicMetadataFetcherSpy.getPositionForTime(tp0, ts), p9988);
    assertEquals(pubSubConsumerPool.size(), 1);

    // test when offsetForTime returns null
    when(consumerMock.getPositionByTimestamp(eq(tp0), eq(ts), any(Duration.class))).thenReturn(null);
    assertEquals(topicMetadataFetcherSpy.getPositionForTime(tp0, ts), endPosition);
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

  @Test
  public void testInvalidateKeyForPubSubTopic() {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    PubSubTopicPartition tp1 = new PubSubTopicPartitionImpl(pubSubTopic, 1);
    PubSubTopic otherTopic = pubSubTopicRepository.getTopic("otherTopic");
    PubSubTopicPartition otherTp = new PubSubTopicPartitionImpl(otherTopic, 0);

    // Populate caches with test data
    TopicMetadataFetcher spyFetcher = spy(topicMetadataFetcher);

    // Add entries to topic existence cache
    spyFetcher.getTopicExistenceCache().put(pubSubTopic, spyFetcher.new ValueAndExpiryTime<>(true));
    spyFetcher.getTopicExistenceCache().put(otherTopic, spyFetcher.new ValueAndExpiryTime<>(true));

    // Add entries to latest position cache
    spyFetcher.getLatestPositionCache()
        .put(tp0, spyFetcher.new ValueAndExpiryTime<>(ApacheKafkaOffsetPosition.of(100L)));
    spyFetcher.getLatestPositionCache()
        .put(tp1, spyFetcher.new ValueAndExpiryTime<>(ApacheKafkaOffsetPosition.of(200L)));
    spyFetcher.getLatestPositionCache()
        .put(otherTp, spyFetcher.new ValueAndExpiryTime<>(ApacheKafkaOffsetPosition.of(300L)));

    // Verify initial state
    assertEquals(spyFetcher.getTopicExistenceCache().size(), 2);
    assertEquals(spyFetcher.getLatestPositionCache().size(), 3);
    assertTrue(spyFetcher.getTopicExistenceCache().containsKey(pubSubTopic));
    assertTrue(spyFetcher.getLatestPositionCache().containsKey(tp0));
    assertTrue(spyFetcher.getLatestPositionCache().containsKey(tp1));

    // Test invalidateKey for PubSubTopic
    spyFetcher.invalidateKey(pubSubTopic);

    // Verify topic existence cache entry is removed
    assertFalse(spyFetcher.getTopicExistenceCache().containsKey(pubSubTopic));
    assertTrue(spyFetcher.getTopicExistenceCache().containsKey(otherTopic));

    // Verify all partition entries for the topic are removed
    assertFalse(spyFetcher.getLatestPositionCache().containsKey(tp0));
    assertFalse(spyFetcher.getLatestPositionCache().containsKey(tp1));
    assertTrue(spyFetcher.getLatestPositionCache().containsKey(otherTp));

    // Verify final cache sizes
    assertEquals(spyFetcher.getTopicExistenceCache().size(), 1);
    assertEquals(spyFetcher.getLatestPositionCache().size(), 1);
  }

  @Test
  public void testInvalidateKeyForPubSubTopicPartition() {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    PubSubTopicPartition tp1 = new PubSubTopicPartitionImpl(pubSubTopic, 1);

    TopicMetadataFetcher spyFetcher = spy(topicMetadataFetcher);

    // Add entries to latest position cache
    spyFetcher.getLatestPositionCache()
        .put(tp0, spyFetcher.new ValueAndExpiryTime<>(ApacheKafkaOffsetPosition.of(100L)));
    spyFetcher.getLatestPositionCache()
        .put(tp1, spyFetcher.new ValueAndExpiryTime<>(ApacheKafkaOffsetPosition.of(200L)));

    // Verify initial state
    assertEquals(spyFetcher.getLatestPositionCache().size(), 2);
    assertTrue(spyFetcher.getLatestPositionCache().containsKey(tp0));
    assertTrue(spyFetcher.getLatestPositionCache().containsKey(tp1));

    // Test invalidateKey for specific partition
    spyFetcher.invalidateKey(tp0);

    // Verify only the specified partition is removed
    assertFalse(spyFetcher.getLatestPositionCache().containsKey(tp0));
    assertTrue(spyFetcher.getLatestPositionCache().containsKey(tp1));
    assertEquals(spyFetcher.getLatestPositionCache().size(), 1);
  }

  @Test
  public void testGetLatestPositionCached() {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);

    TopicMetadataFetcher spyFetcher = spy(topicMetadataFetcher);
    ApacheKafkaOffsetPosition expectedPosition = ApacheKafkaOffsetPosition.of(500L);

    // Mock getLatestPositionWithRetries to return expected position
    doReturn(expectedPosition).when(spyFetcher).getLatestPositionWithRetries(eq(tp0), any(Integer.class));

    // Test cache miss - should fetch and cache the position
    PubSubPosition result = spyFetcher.getLatestPositionCached(tp0);
    assertEquals(result, expectedPosition);

    // Verify the value was cached
    assertTrue(spyFetcher.getLatestPositionCache().containsKey(tp0));
    assertEquals(spyFetcher.getLatestPositionCache().get(tp0).getValue(), expectedPosition);

    // Verify getLatestPositionWithRetries was called once
    verify(spyFetcher, times(1)).getLatestPositionWithRetries(eq(tp0), any(Integer.class));

    // Test cache hit - should return cached value without calling getLatestPositionWithRetries again
    PubSubPosition cachedResult = spyFetcher.getLatestPositionCached(tp0);
    assertEquals(cachedResult, expectedPosition);

    // Verify getLatestPositionWithRetries was not called again
    verify(spyFetcher, times(1)).getLatestPositionWithRetries(eq(tp0), any(Integer.class));
  }

  @Test
  public void testGetLatestPositionCachedWithException() {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);

    TopicMetadataFetcher spyFetcher = spy(topicMetadataFetcher);

    // Test PubSubTopicDoesNotExistException - should return LATEST
    doThrow(new PubSubTopicDoesNotExistException("Topic does not exist")).when(spyFetcher)
        .getLatestPositionWithRetries(eq(tp0), any(Integer.class));

    PubSubPosition result = spyFetcher.getLatestPositionCached(tp0);
    assertEquals(result, PubSubSymbolicPosition.LATEST);

    // Test PubSubOpTimeoutException - should return LATEST
    doThrow(new PubSubOpTimeoutException("Timeout")).when(spyFetcher)
        .getLatestPositionWithRetries(eq(tp0), any(Integer.class));

    result = spyFetcher.getLatestPositionCached(tp0);
    assertEquals(result, PubSubSymbolicPosition.LATEST);
  }

  @Test
  public void testPopulateCacheWithLatestOffset() {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);

    TopicMetadataFetcher spyFetcher = spy(topicMetadataFetcher);
    ApacheKafkaOffsetPosition expectedPosition = ApacheKafkaOffsetPosition.of(750L);

    // Mock getLatestPositionWithRetries to return expected position
    doReturn(expectedPosition).when(spyFetcher).getLatestPositionWithRetries(eq(tp0), any(Integer.class));

    // Test populateCacheWithLatestOffset - should populate cache asynchronously
    spyFetcher.populateCacheWithLatestOffset(tp0);

    // Wait for async operation to complete
    waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      assertTrue(spyFetcher.getLatestPositionCache().containsKey(tp0));
      assertEquals(spyFetcher.getLatestPositionCache().get(tp0).getValue(), expectedPosition);
    });
  }

  @Test
  public void testGetLatestPositionCachedWithExpiredEntry() {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);

    TopicMetadataFetcher spyFetcher = spy(topicMetadataFetcher);
    ApacheKafkaOffsetPosition initialPosition = ApacheKafkaOffsetPosition.of(100L);
    ApacheKafkaOffsetPosition updatedPosition = ApacheKafkaOffsetPosition.of(200L);

    // Pre-populate cache with expired entry
    ValueAndExpiryTime<PubSubPosition> expiredEntry = spyFetcher.new ValueAndExpiryTime<>(initialPosition);
    expiredEntry.setExpiryTimeNs(System.nanoTime() - TimeUnit.SECONDS.toNanos(1)); // Set to expired (1 second ago)
    spyFetcher.getLatestPositionCache().put(tp0, expiredEntry);

    // Mock async update to return updated position - use a delayed future to prevent immediate completion
    CompletableFuture<PubSubPosition> delayedFuture = new CompletableFuture<>();
    doReturn(delayedFuture).when(spyFetcher).getLatestOffsetWithRetriesAsync(eq(tp0), any(Integer.class));

    // Call getLatestPositionCached - with expired entry, it should return cached value but trigger async update
    PubSubPosition result = spyFetcher.getLatestPositionCached(tp0);

    // The result should be the cached value since computeIfAbsent doesn't replace existing entries
    assertEquals(result, initialPosition);

    // Verify that async update was triggered
    verify(spyFetcher, times(1)).getLatestOffsetWithRetriesAsync(eq(tp0), any(Integer.class));

    // Complete the future to simulate async update completion
    delayedFuture.complete(updatedPosition);

    // Wait for async update to complete and verify cache was updated
    waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      assertEquals(spyFetcher.getLatestPositionCache().get(tp0).getValue(), updatedPosition);
    });
  }

  @Test
  public void testGetEarliestPositionCachedScenarios() {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);

    TopicMetadataFetcher spyFetcher = spy(topicMetadataFetcher);
    ApacheKafkaOffsetPosition expectedPosition = ApacheKafkaOffsetPosition.of(0L);

    // Case 1: Cache miss - should fetch and populate cache
    Map<PubSubTopicPartition, PubSubPosition> tp0Map = new HashMap<>();
    tp0Map.put(tp0, expectedPosition);
    when(consumerMock.beginningPositions(eq(Collections.singleton(tp0)), any(Duration.class))).thenReturn(tp0Map);
    PubSubPosition result1 = spyFetcher.getEarliestPositionCached(tp0);
    assertEquals(result1, expectedPosition);
    assertTrue(spyFetcher.getEarliestPositionCache().containsKey(tp0));
    assertEquals(spyFetcher.getEarliestPositionCache().get(tp0).getValue(), expectedPosition);

    // Case 2: Cache hit - should return cached value without fetching again
    PubSubPosition result2 = spyFetcher.getEarliestPositionCached(tp0);
    assertEquals(result2, expectedPosition);
    verify(consumerMock, times(1)).beginningPositions(eq(Collections.singleton(tp0)), any(Duration.class)); // Should
                                                                                                            // only be
                                                                                                            // called
                                                                                                            // once

    // Case 3: Multiple partitions - each should be cached independently
    PubSubTopicPartition tp1 = new PubSubTopicPartitionImpl(pubSubTopic, 1);
    ApacheKafkaOffsetPosition expectedPosition1 = ApacheKafkaOffsetPosition.of(10L);
    Map<PubSubTopicPartition, PubSubPosition> tp1Map = new HashMap<>();
    tp1Map.put(tp1, expectedPosition1);
    when(consumerMock.beginningPositions(eq(Collections.singleton(tp1)), any(Duration.class))).thenReturn(tp1Map);
    PubSubPosition result3 = spyFetcher.getEarliestPositionCached(tp1);
    assertEquals(result3, expectedPosition1);
    assertEquals(spyFetcher.getEarliestPositionCache().size(), 2);

    assertEquals(pubSubConsumerPool.size(), 1);
  }

  @Test
  public void testGetEarliestPositionCachedWithException() {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);

    TopicMetadataFetcher spyFetcher = spy(topicMetadataFetcher);

    // Case 1: PubSubTopicDoesNotExistException - should return EARLIEST symbolic position
    when(consumerMock.beginningPositions(eq(Collections.singleton(tp0)), any(Duration.class)))
        .thenThrow(new PubSubTopicDoesNotExistException("Topic does not exist"));
    PubSubPosition result1 = spyFetcher.getEarliestPositionCached(tp0);
    assertEquals(result1, PubSubSymbolicPosition.EARLIEST);

    // Case 2: PubSubOpTimeoutException - should return EARLIEST symbolic position
    spyFetcher.getEarliestPositionCache().clear();
    when(consumerMock.beginningPositions(eq(Collections.singleton(tp0)), any(Duration.class)))
        .thenThrow(new PubSubOpTimeoutException("Timeout"));
    PubSubPosition result2 = spyFetcher.getEarliestPositionCached(tp0);
    assertEquals(result2, PubSubSymbolicPosition.EARLIEST);

    assertEquals(pubSubConsumerPool.size(), 1);
  }

  @Test
  public void testGetEarliestPositionCachedNonBlockingScenarios() {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);

    TopicMetadataFetcher spyFetcher = spy(topicMetadataFetcher);
    ApacheKafkaOffsetPosition expectedPosition = ApacheKafkaOffsetPosition.of(0L);

    // Case 1: Cache miss - should return EARLIEST and trigger async population
    PubSubPosition result1 = spyFetcher.getEarliestPositionCachedNonBlocking(tp0);
    assertEquals(result1, PubSubSymbolicPosition.EARLIEST);

    // Case 2: After async population completes, should return cached value
    Map<PubSubTopicPartition, PubSubPosition> tp0Map = new HashMap<>();
    tp0Map.put(tp0, expectedPosition);
    when(consumerMock.beginningPositions(eq(Collections.singleton(tp0)), any(Duration.class))).thenReturn(tp0Map);
    spyFetcher.getEarliestPositionCache().put(tp0, spyFetcher.new ValueAndExpiryTime<>(expectedPosition));
    PubSubPosition result2 = spyFetcher.getEarliestPositionCachedNonBlocking(tp0);
    assertEquals(result2, expectedPosition);

    // Case 3: Expired cache entry - should return cached value but trigger async update
    ValueAndExpiryTime<PubSubPosition> expiredEntry = spyFetcher.new ValueAndExpiryTime<>(expectedPosition);
    expiredEntry.setExpiryTimeNs(System.nanoTime() - TimeUnit.SECONDS.toNanos(1));
    spyFetcher.getEarliestPositionCache().put(tp0, expiredEntry);
    PubSubPosition result3 = spyFetcher.getEarliestPositionCachedNonBlocking(tp0);
    assertEquals(result3, expectedPosition);

    assertEquals(pubSubConsumerPool.size(), 1);
  }

  @Test
  public void testGetEarliestPositionNoRetry() throws ExecutionException, InterruptedException {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);

    TopicMetadataFetcher spyFetcher = spy(topicMetadataFetcher);
    ApacheKafkaOffsetPosition expectedPosition = ApacheKafkaOffsetPosition.of(0L);

    // Case 1: Successful async fetch
    Map<PubSubTopicPartition, PubSubPosition> tp0Map = new HashMap<>();
    tp0Map.put(tp0, expectedPosition);
    when(consumerMock.beginningPositions(eq(Collections.singleton(tp0)), any(Duration.class))).thenReturn(tp0Map);
    CompletableFuture<PubSubPosition> future1 = spyFetcher.getEarliestPositionNoRetry(tp0);
    PubSubPosition result1 = future1.get();
    assertEquals(result1, expectedPosition);

    // Case 2: Multiple concurrent calls should all complete successfully
    CompletableFuture<PubSubPosition> future2 = spyFetcher.getEarliestPositionNoRetry(tp0);
    CompletableFuture<PubSubPosition> future3 = spyFetcher.getEarliestPositionNoRetry(tp0);
    PubSubPosition result2 = future2.get();
    PubSubPosition result3 = future3.get();
    assertEquals(result2, expectedPosition);
    assertEquals(result3, expectedPosition);

    assertEquals(pubSubConsumerPool.size(), 1);
  }

  @Test
  public void testInvalidateKeyRemovesBothCaches() {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);

    TopicMetadataFetcher spyFetcher = spy(topicMetadataFetcher);
    ApacheKafkaOffsetPosition earliestPosition = ApacheKafkaOffsetPosition.of(0L);
    ApacheKafkaOffsetPosition latestPosition = ApacheKafkaOffsetPosition.of(1000L);

    // Case 1: Populate both caches
    spyFetcher.getEarliestPositionCache().put(tp0, spyFetcher.new ValueAndExpiryTime<>(earliestPosition));
    spyFetcher.getLatestPositionCache().put(tp0, spyFetcher.new ValueAndExpiryTime<>(latestPosition));
    assertEquals(spyFetcher.getEarliestPositionCache().size(), 1);
    assertEquals(spyFetcher.getLatestPositionCache().size(), 1);

    // Case 2: Invalidate partition key - should remove from both caches
    spyFetcher.invalidateKey(tp0);
    assertEquals(spyFetcher.getEarliestPositionCache().size(), 0);
    assertEquals(spyFetcher.getLatestPositionCache().size(), 0);

    // Case 3: Invalidate topic key - should remove all partitions from both caches
    PubSubTopicPartition tp1 = new PubSubTopicPartitionImpl(pubSubTopic, 1);
    spyFetcher.getEarliestPositionCache().put(tp0, spyFetcher.new ValueAndExpiryTime<>(earliestPosition));
    spyFetcher.getEarliestPositionCache().put(tp1, spyFetcher.new ValueAndExpiryTime<>(earliestPosition));
    spyFetcher.getLatestPositionCache().put(tp0, spyFetcher.new ValueAndExpiryTime<>(latestPosition));
    spyFetcher.getLatestPositionCache().put(tp1, spyFetcher.new ValueAndExpiryTime<>(latestPosition));
    assertEquals(spyFetcher.getEarliestPositionCache().size(), 2);
    assertEquals(spyFetcher.getLatestPositionCache().size(), 2);

    spyFetcher.invalidateKey(pubSubTopic);
    assertEquals(spyFetcher.getEarliestPositionCache().size(), 0);
    assertEquals(spyFetcher.getLatestPositionCache().size(), 0);

    assertEquals(pubSubConsumerPool.size(), 1);
  }

  @Test
  public void testGetEarliestPositionCachedWithExpiredEntry() {
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);

    TopicMetadataFetcher spyFetcher = spy(topicMetadataFetcher);
    ApacheKafkaOffsetPosition initialPosition = ApacheKafkaOffsetPosition.of(0L);
    ApacheKafkaOffsetPosition updatedPosition = ApacheKafkaOffsetPosition.of(10L);

    // Case 1: Pre-populate cache with expired entry
    ValueAndExpiryTime<PubSubPosition> expiredEntry = spyFetcher.new ValueAndExpiryTime<>(initialPosition);
    expiredEntry.setExpiryTimeNs(System.nanoTime() - TimeUnit.SECONDS.toNanos(1));
    spyFetcher.getEarliestPositionCache().put(tp0, expiredEntry);

    // Case 2: Mock async update to return updated position
    CompletableFuture<PubSubPosition> delayedFuture = new CompletableFuture<>();
    doReturn(delayedFuture).when(spyFetcher).getEarliestPositionNoRetry(eq(tp0));

    // Case 3: Call getEarliestPositionCached - should return cached value but trigger async update
    PubSubPosition result = spyFetcher.getEarliestPositionCached(tp0);
    assertEquals(result, initialPosition);
    verify(spyFetcher, times(1)).getEarliestPositionNoRetry(eq(tp0));

    // Case 4: Complete the future and verify cache was updated
    delayedFuture.complete(updatedPosition);
    waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      assertEquals(spyFetcher.getEarliestPositionCache().get(tp0).getValue(), updatedPosition);
    });

    assertEquals(pubSubConsumerPool.size(), 1);
  }
}
