package com.linkedin.davinci;

import static org.mockito.AdditionalAnswers.answerVoid;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.DaVinciIngestionBackend;
import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreBackendTest {
  Store store;
  Version version1;
  Version version2;
  File baseDataPath;
  DaVinciBackend backend;
  StoreBackend storeBackend;
  Map<String, VersionBackend> versionMap;
  MetricsRepository metricsRepository;
  StorageService storageService;
  DaVinciIngestionBackend ingestionBackend;
  StorageEngineBackedCompressorFactory compressorFactory;

  @BeforeMethod
  void setUp() {
    baseDataPath = Utils.getTempDataDirectory();
    VeniceProperties backendConfig = new PropertyBuilder().put(ConfigKeys.CLUSTER_NAME, "test-cluster")
        .put(ConfigKeys.ZOOKEEPER_ADDRESS, "test-zookeeper")
        .put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, "test-kafka")
        .put(ConfigKeys.DATA_BASE_PATH, baseDataPath.getAbsolutePath())
        .build();

    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    doAnswer(answerVoid(Runnable::run)).when(executor).execute(any());

    versionMap = new HashMap<>();
    metricsRepository = new MetricsRepository();
    storageService = mock(StorageService.class);
    ingestionBackend = mock(DaVinciIngestionBackend.class);
    compressorFactory = mock(StorageEngineBackedCompressorFactory.class);
    when(ingestionBackend.getStorageService()).thenReturn(storageService);
    backend = mock(DaVinciBackend.class);
    when(backend.getExecutor()).thenReturn(executor);
    when(backend.getConfigLoader()).thenReturn(new VeniceConfigLoader(backendConfig));
    when(backend.getMetricsRepository()).thenReturn(metricsRepository);
    when(backend.getStoreRepository()).thenReturn(mock(SubscriptionBasedReadOnlyStoreRepository.class));
    when(backend.getStorageService()).thenReturn(storageService);
    when(backend.getIngestionService()).thenReturn(mock(StoreIngestionService.class));
    when(backend.getVersionByTopicMap()).thenReturn(versionMap);
    when(backend.getVeniceLatestNonFaultyVersion(anyString(), anySet())).thenCallRealMethod();
    when(backend.getVeniceCurrentVersion(anyString())).thenCallRealMethod();
    when(backend.getIngestionBackend()).thenReturn(ingestionBackend);
    when(backend.getCompressorFactory()).thenReturn(compressorFactory);
    doCallRealMethod().when(backend).handleStoreChanged(any());

    store = new ZKStore(
        "test-store",
        null,
        0,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
    version1 = new VersionImpl(store.getName(), store.peekNextVersion().getNumber(), null, 5);
    store.addVersion(version1);
    version2 = new VersionImpl(store.getName(), store.peekNextVersion().getNumber(), null, 3);
    store.addVersion(version2);
    store.setCurrentVersion(version1.getNumber());
    when(backend.getStoreRepository().getStoreOrThrow(store.getName())).thenReturn(store);

    storeBackend = new StoreBackend(backend, store.getName());
    when(backend.getStoreOrThrow(store.getName())).thenReturn(storeBackend);
  }

  private double getMetric(String metricName) {
    Metric metric = metricsRepository.getMetric("." + store.getName() + "--" + metricName);
    assertNotNull(metric, "Expected metric " + metricName + " not found.");
    return metric.value();
  }

  @Test
  void testSubscribeEmptyStore() {
    store.setVersions(Collections.emptyList());
    store.setCurrentVersion(Store.NON_EXISTING_VERSION);
    assertThrows(VeniceException.class, () -> storeBackend.subscribe(ComplementSet.universalSet()));
  }

  @Test
  void testSubscribeCurrentVersion() throws Exception {
    int partition = 0;
    long v1SubscribeDurationMs = 100;
    long v2SubscribeDurationMs = 200;
    version1.setAge(Duration.ofHours(1));
    version2.setAge(Duration.ofMinutes(5));
    assertEquals(getMetric("data_age_ms.Gauge"), Double.NaN);

    // Expecting to subscribe to version1 and that version2 is a future version.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(partition));
    TimeUnit.MILLISECONDS.sleep(v1SubscribeDurationMs);
    versionMap.get(version1.kafkaTopicName()).completePartition(partition);
    subscribeResult.get(0, TimeUnit.SECONDS);
    // Verify that subscribe selected the current version by default.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }

    assertEquals(getMetric("current_version_number.Gauge"), (double) version1.getNumber());
    assertEquals(getMetric("future_version_number.Gauge"), (double) version2.getNumber());
    assertTrue(Math.abs(getMetric("data_age_ms.Gauge") - version1.getAge().toMillis()) < 1000);
    assertTrue(Math.abs(getMetric("subscribe_duration_ms.Avg") - v1SubscribeDurationMs) < 50);

    // Simulate future version ingestion is complete.
    TimeUnit.MILLISECONDS.sleep(v2SubscribeDurationMs - v1SubscribeDurationMs);
    versionMap.get(version2.kafkaTopicName()).completePartition(partition);

    // Verify that future version did not become current even if ingestion is complete.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }
    // Mark the version 2 as current.
    store.setCurrentVersion(version2.getNumber());
    backend.handleStoreChanged(storeBackend);

    // Verify that future version became current once ingestion is complete.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version2.getNumber());
    }

    assertEquals(getMetric("current_version_number.Gauge"), (double) version2.getNumber());
    assertTrue(Math.abs(getMetric("data_age_ms.Gauge") - version2.getAge().toMillis()) < 1000);
    assertTrue(
        Math.abs(getMetric("subscribe_duration_ms.Avg") - (v1SubscribeDurationMs + v2SubscribeDurationMs) / 2.) < 50);
    assertTrue(Math.abs(getMetric("subscribe_duration_ms.Max") - v2SubscribeDurationMs) < 50);
  }

  @Test
  void testSubscribeSlowCurrentVersion() throws Exception {
    int partition = 0;
    // Expecting to subscribe to version1 and that version2 is a future version.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(partition));
    assertFalse(subscribeResult.isDone());
    // Verify that subscribe selected the current version by default.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }

    // Simulate future version ingestion is complete.
    versionMap.get(version2.kafkaTopicName()).completePartition(partition);

    // Verify that future version did not become current even if ingestion is complete.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }
    // Mark the version 2 as current.
    store.setCurrentVersion(version2.getNumber());
    backend.handleStoreChanged(storeBackend);

    // Verify that future version became current once ingestion is complete.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version2.getNumber());
    }
  }

  @Test
  void testSubscribeWithoutCurrentVersion() throws Exception {
    int partition = 1;
    store.setCurrentVersion(Store.NON_EXISTING_VERSION);
    backend.handleStoreChanged(storeBackend);

    // Expecting to subscribe to the latest version (version2).
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(partition));
    versionMap.get(version2.kafkaTopicName()).completePartition(partition);
    subscribeResult.get(0, TimeUnit.SECONDS);
    // Verify that subscribe selected the latest version as current.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version2.getNumber());
    }
  }

  @Test
  void testSubscribeBootstrapVersion() throws Exception {
    Version version3 = new VersionImpl(store.getName(), store.peekNextVersion().getNumber(), null, 15);
    store.addVersion(version3);
    store.setCurrentVersion(version2.getNumber());
    backend.handleStoreChanged(storeBackend);

    int partition = 2;
    // Expecting to subscribe to the specified version (version1), which is neither current nor latest.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(partition), Optional.of(version1));
    versionMap.get(version1.kafkaTopicName()).completePartition(partition);
    subscribeResult.get(0, TimeUnit.SECONDS);
    // Verify that subscribe selected the specified version as current.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }

    // Simulate future version ingestion is complete.
    versionMap.get(version2.kafkaTopicName()).completePartition(partition);
    // Verify that future version became current once ingestion is complete.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version2.getNumber());
    }
  }

  @Test
  void testFutureVersionFailure() throws Exception {
    int partition = 1;
    // Expecting to subscribe to version1 and that version2 is a future version.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(partition));
    versionMap.get(version1.kafkaTopicName()).completePartition(partition);
    subscribeResult.get(0, TimeUnit.SECONDS);
    assertTrue(versionMap.containsKey(version2.kafkaTopicName()));

    // Simulate future version kill and removal from Venice.
    store.deleteVersion(version2.getNumber());
    backend.handleStoreChanged(storeBackend);
    // Verify that corresponding Version Backend is deleted exactly once.
    assertFalse(versionMap.containsKey(version2.kafkaTopicName()));
    verify(ingestionBackend, times(1)).removeStorageEngine(eq(version2.kafkaTopicName()));

    // Simulate new version push and subsequent ingestion failure.
    Version version3 = new VersionImpl(store.getName(), store.peekNextVersion().getNumber(), null, 15);
    store.addVersion(version3);
    backend.handleStoreChanged(storeBackend);

    // Simulate new version push while faulty future version is being ingested.
    Version version4 = new VersionImpl(store.getName(), store.peekNextVersion().getNumber(), null, 20);
    store.addVersion(version4);
    backend.handleStoreChanged(storeBackend);

    versionMap.get(version3.kafkaTopicName()).completePartitionExceptionally(partition, new Exception());
    // Verify that neither of the bad versions became current.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }

    versionMap.get(version4.kafkaTopicName()).completePartition(partition);
    // Verify that version 4 did not become current even if ingestion is complete.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }
    // Mark the version 4 as current.
    store.setCurrentVersion(version4.getNumber());
    backend.handleStoreChanged(storeBackend);

    // Verify that successfully ingested version became current.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version4.getNumber());
    }

    // Simulate new version push and subsequent ingestion failure.
    Version version5 = new VersionImpl(store.getName(), store.peekNextVersion().getNumber(), null, 30);
    store.addVersion(version5);
    backend.handleStoreChanged(storeBackend);
    versionMap.get(version5.kafkaTopicName()).completePartitionExceptionally(partition, new Exception());
    // Verify that corresponding Version Backend is deleted exactly once.
    assertFalse(versionMap.containsKey(version5.kafkaTopicName()));
    verify(ingestionBackend, times(1)).removeStorageEngine(eq(version5.kafkaTopicName()));

    // Verify that faulty version will not be tried again.
    storeBackend.trySubscribeDaVinciFutureVersion();
    assertFalse(versionMap.containsKey(version5.kafkaTopicName()));
  }

  @Test
  void testSubscribeUnsubscribe() throws Exception {
    // Simulate concurrent unsubscribe while subscribe is pending.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(0, 1));
    versionMap.get(version1.kafkaTopicName()).completePartition(0);
    assertFalse(subscribeResult.isDone());
    storeBackend.unsubscribe(ComplementSet.of(1));
    // Verify that unsubscribe completed pending subscribe without failing it.
    subscribeResult.get(0, TimeUnit.SECONDS);
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertTrue(versionRef.get().isPartitionReadyToServe(0));
      assertFalse(versionRef.get().isPartitionReadyToServe(1));
    }

    // Simulate unsubscribe from all partitions while future version ingestion is pending.
    subscribeResult = storeBackend.subscribe(ComplementSet.universalSet());
    storeBackend.unsubscribe(ComplementSet.universalSet());
    subscribeResult.get(0, TimeUnit.SECONDS);
    // Verify that all versions were deleted because subscription set became empty.
    assertTrue(versionMap.isEmpty());
    assertEquals(FileUtils.sizeOfDirectory(baseDataPath), 0);
    verify(ingestionBackend, times(store.getVersions().size())).removeStorageEngine(any());
    verify(compressorFactory, times(store.getVersions().size())).removeVersionSpecificCompressor(any());
  }

  @Test
  void testSubscribeClose() {
    // Simulate concurrent store close while subscribe is pending.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.universalSet());
    storeBackend.close();
    // Verify that store close aborted pending subscribe, but none of the versions was deleted.
    assertThrows(CompletionException.class, () -> subscribeResult.getNow(null));
    verify(backend.getStorageService(), never()).removeStorageEngine(any());
  }

  @Test
  void testSubscribeDelete() {
    // Simulate concurrent store deletion while subscription is pending.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.universalSet());
    storeBackend.delete();
    // Verify that store delete aborted pending subscribe and that all versions were deleted exactly once.
    assertThrows(CompletionException.class, () -> subscribeResult.getNow(null));
    assertTrue(versionMap.isEmpty());
    assertEquals(FileUtils.sizeOfDirectory(baseDataPath), 0);
    verify(ingestionBackend, times(store.getVersions().size())).removeStorageEngine(any());
  }

  @Test
  void testRollbackAndRollForward() {
    int partition = 1;
    // Expecting to subscribe to the latest version (v1).
    storeBackend.subscribe(ComplementSet.of(partition));
    versionMap.get(version1.kafkaTopicName()).completePartition(partition);

    store.setCurrentVersion(version2.getNumber());
    backend.handleStoreChanged(storeBackend);
    versionMap.get(version2.kafkaTopicName()).completePartition(partition);

    Version version3 = new VersionImpl(store.getName(), store.peekNextVersion().getNumber(), null, 3);
    store.addVersion(version3);
    backend.handleStoreChanged(storeBackend);

    versionMap.get(version3.kafkaTopicName()).completePartition(partition);
    store.setCurrentVersion(version3.getNumber());
    backend.handleStoreChanged(storeBackend);
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version3.getNumber());
    }

    // Rollback happens here, expecting Da Vinci to switch back to v1.
    store.setCurrentVersion(1);
    backend.handleStoreChanged(storeBackend);
    versionMap.get(version1.kafkaTopicName()).completePartition(partition);
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }
    versionMap.get(version2.kafkaTopicName()).completePartition(partition);

    store.setCurrentVersion(3);
    backend.handleStoreChanged(storeBackend);
    versionMap.get(version3.kafkaTopicName()).completePartition(partition);
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version3.getNumber());
    }
  }
}
