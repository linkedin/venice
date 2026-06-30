package com.linkedin.davinci;

import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.mockito.AdditionalAnswers.answerVoid;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.eq;
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

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.IngestionBackend;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatLagMonitorAction;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
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
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
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
  AsyncGauge.AsyncGaugeExecutor gaugeExecutor;
  MetricsRepository metricsRepository;
  StorageService storageService;
  IngestionBackend ingestionBackend;
  StorageEngineBackedCompressorFactory compressorFactory;
  HeartbeatMonitoringService heartbeatMonitoringService;

  @BeforeMethod
  void setUp() {
    baseDataPath = Utils.getTempDataDirectory();
    VeniceProperties backendConfig = new PropertyBuilder().put(ConfigKeys.CLUSTER_NAME, "test-cluster")
        .put(ConfigKeys.ZOOKEEPER_ADDRESS, "test-zookeeper")
        .put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, "test-kafka")
        .put(ConfigKeys.DATA_BASE_PATH, baseDataPath.getAbsolutePath())
        .put(ConfigKeys.LOCAL_REGION_NAME, "dc-0")
        .put(ConfigKeys.DAVINCI_PAUSED_SIT_ENABLED, "true")
        .build();

    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    doAnswer(answerVoid(Runnable::run)).when(executor).execute(any());

    versionMap = new HashMap<>();
    // Use a dedicated executor to avoid contention with the shared DEFAULT_ASYNC_GAUGE_EXECUTOR in CI
    gaugeExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
    metricsRepository = new MetricsRepository(new MetricConfig(gaugeExecutor));
    storageService = mock(StorageService.class);
    ingestionBackend = mock(IngestionBackend.class);
    compressorFactory = mock(StorageEngineBackedCompressorFactory.class);
    backend = mock(DaVinciBackend.class);
    heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    when(backend.getExecutor()).thenReturn(executor);
    when(backend.getConfigLoader()).thenReturn(new VeniceConfigLoader(backendConfig));
    when(backend.getMetricsRepository()).thenReturn(metricsRepository);
    when(backend.getStoreRepository()).thenReturn(mock(SubscriptionBasedReadOnlyStoreRepository.class));
    when(backend.getStorageService()).thenReturn(storageService);
    when(backend.getIngestionService()).thenReturn(mock(KafkaStoreIngestionService.class));
    when(backend.getVersionByTopicMap()).thenReturn(versionMap);
    when(backend.getVeniceLatestNonFaultyVersion(anyString(), anySet())).thenCallRealMethod();
    when(backend.getVeniceCurrentVersion(anyString())).thenCallRealMethod();
    when(backend.getIngestionBackend()).thenReturn(ingestionBackend);
    when(backend.getCompressorFactory()).thenReturn(compressorFactory);
    when(backend.hasCurrentVersionBootstrapping()).thenReturn(false);
    when(backend.getHeartbeatMonitoringService()).thenReturn(heartbeatMonitoringService);
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
    version1 = new VersionImpl(store.getName(), store.peekNextVersionNumber(), null, 5);
    store.addVersion(version1);
    version2 = new VersionImpl(store.getName(), store.peekNextVersionNumber(), null, 3);
    store.addVersion(version2);
    store.setCurrentVersion(version1.getNumber());
    when(backend.getStoreRepository().getStoreOrThrow(store.getName())).thenReturn(store);

    storeBackend = new StoreBackend(backend, store.getName());
    when(backend.getStoreOrThrow(store.getName())).thenReturn(storeBackend);
  }

  @org.testng.annotations.AfterMethod
  void tearDown() throws Exception {
    metricsRepository.close();
    gaugeExecutor.close();
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
    verify(heartbeatMonitoringService, times(1)).updateLagMonitor(
        eq(version1.kafkaTopicName()),
        eq(partition),
        eq(HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR),
        anyString());
    TimeUnit.MILLISECONDS.sleep(v1SubscribeDurationMs);
    versionMap.get(version1.kafkaTopicName()).completePartition(partition);
    subscribeResult.get(3, TimeUnit.SECONDS);
    // Verify that subscribe selected the current version by default.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }
    // Partition futures and metrics may be completed asynchronously in callback threads.
    waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      assertTrue(versionMap.get(version1.kafkaTopicName()).areAllPartitionFuturesCompletedSuccessfully());
      assertEquals(getMetric("current_version_number.Gauge"), (double) version1.getNumber());
      assertEquals(getMetric("future_version_number.Gauge"), (double) version2.getNumber());
      assertTrue(Math.abs(getMetric("data_age_ms.Gauge") - version1.getAge().toMillis()) < 1000);
      // Duration is >= the sleep we waited; use a lower-bound check that tolerates slow CI machines.
      assertTrue(getMetric("subscribe_duration_ms.Avg") >= v1SubscribeDurationMs);
    });

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

    // Version swap and metric recording happen asynchronously after handleStoreChanged.
    waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      assertEquals(getMetric("current_version_number.Gauge"), (double) version2.getNumber());
      assertTrue(Math.abs(getMetric("data_age_ms.Gauge") - version2.getAge().toMillis()) < 1000);
      // Avg should be >= v1 (both v1 and v2 recorded); Max should be >= v2 (v2 took longer).
      // Use lower-bound checks that tolerate slow CI machines where sleep durations are inflated.
      assertTrue(getMetric("subscribe_duration_ms.Avg") >= v1SubscribeDurationMs);
      assertTrue(getMetric("subscribe_duration_ms.Max") >= v2SubscribeDurationMs);
    });
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
    subscribeResult.get(3, TimeUnit.SECONDS);
    // Verify that subscribe selected the latest version as current.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version2.getNumber());
    }
  }

  @Test
  void testSubscribeBootstrapVersion() throws Exception {
    Version version3 = new VersionImpl(store.getName(), store.peekNextVersionNumber(), null, 15);
    store.addVersion(version3);
    store.setCurrentVersion(version2.getNumber());
    backend.handleStoreChanged(storeBackend);

    int partition = 2;
    // Expecting to subscribe to the specified version (version1), which is neither current nor latest.
    CompletableFuture subscribeResult =
        storeBackend.subscribe(ComplementSet.of(partition), Optional.of(version1), null);
    versionMap.get(version1.kafkaTopicName()).completePartition(partition);
    subscribeResult.get(3, TimeUnit.SECONDS);
    // Verify that subscribe selected the specified version as current.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }

    // Simulate future version ingestion is complete.
    versionMap.get(version2.kafkaTopicName()).completePartition(partition);
    // Verify that future version became current once ingestion is complete.
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
        assertEquals(versionRef.get().getVersion().getNumber(), version2.getNumber());
      }
    });
  }

  @Test
  void testSubscribeVersionSpecific() throws Exception {
    when(backend.getStoreClientType(store.getName())).thenReturn(DaVinciBackend.ClientType.VERSION_SPECIFIC);
    storeBackend = spy(storeBackend);
    int partitionCount = 3;

    Version version3 = new VersionImpl(store.getName(), store.peekNextVersionNumber(), null, partitionCount);
    store.addVersion(version3);
    store.setCurrentVersion(version2.getNumber());
    backend.handleStoreChanged(storeBackend);

    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      // Subscribe to the specified version (version1) with version-specific client
      CompletableFuture subscribeResult =
          storeBackend.subscribe(ComplementSet.of(partitionId), Optional.of(version1), null);
      versionMap.get(version1.kafkaTopicName()).completePartition(partitionId);
      subscribeResult.get(3, TimeUnit.SECONDS);
    }
    // Verify that subscribe selected the specified version as current
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }

    verify(storeBackend, never()).trySubscribeDaVinciFutureVersion();

    // Try to subscribe to a new version
    assertThrows(VeniceException.class, () -> storeBackend.subscribe(ComplementSet.of(1), Optional.of(version3), null));
  }

  @Test
  void testFutureVersionFailure() throws Exception {
    int partition = 1;
    // Expecting to subscribe to version1 and that version2 is a future version.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(partition));
    versionMap.get(version1.kafkaTopicName()).completePartition(partition);
    subscribeResult.get(3, TimeUnit.SECONDS);
    assertTrue(versionMap.containsKey(version2.kafkaTopicName()));

    // Simulate future version kill and removal from Venice.
    store.deleteVersion(version2.getNumber());
    backend.handleStoreChanged(storeBackend);
    // Verify that corresponding Version Backend is deleted exactly once.
    assertFalse(versionMap.containsKey(version2.kafkaTopicName()));
    verify(ingestionBackend, times(1)).removeStorageEngine(eq(version2.kafkaTopicName()));

    // Simulate new version push and subsequent ingestion failure.
    Version version3 = new VersionImpl(store.getName(), store.peekNextVersionNumber(), null, 15);
    store.addVersion(version3);
    backend.handleStoreChanged(storeBackend);

    // Simulate new version push while faulty future version is being ingested.
    Version version4 = new VersionImpl(store.getName(), store.peekNextVersionNumber(), null, 20);
    store.addVersion(version4);
    backend.handleStoreChanged(storeBackend);

    versionMap.get(version3.kafkaTopicName()).completePartitionExceptionally(partition, new Exception());
    // Verify that neither of the bad versions became current.
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
        assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
      }
    });

    versionMap.get(version4.kafkaTopicName()).completePartition(partition);
    // Verify that version 4 did not become current even if ingestion is complete.
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
        assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
      }
    });
    // Mark the version 4 as current.
    store.setCurrentVersion(version4.getNumber());
    backend.handleStoreChanged(storeBackend);

    // Verify that successfully ingested version became current.
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
        assertEquals(versionRef.get().getVersion().getNumber(), version4.getNumber());
      }
    });

    // Simulate new version push and subsequent ingestion failure.
    Version version5 = new VersionImpl(store.getName(), store.peekNextVersionNumber(), null, 30);
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
    verify(heartbeatMonitoringService, times(1)).updateLagMonitor(
        eq(version1.kafkaTopicName()),
        eq(0),
        eq(HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR),
        anyString());
    verify(heartbeatMonitoringService, times(1)).updateLagMonitor(
        eq(version1.kafkaTopicName()),
        eq(1),
        eq(HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR),
        anyString());
    versionMap.get(version1.kafkaTopicName()).completePartition(0);
    assertFalse(subscribeResult.isDone());
    storeBackend.unsubscribe(ComplementSet.of(1));
    verify(heartbeatMonitoringService, times(1)).updateLagMonitor(
        eq(version1.kafkaTopicName()),
        eq(1),
        eq(HeartbeatLagMonitorAction.REMOVE_MONITOR),
        anyString());
    verify(heartbeatMonitoringService, times(0)).updateLagMonitor(
        eq(version1.kafkaTopicName()),
        eq(0),
        eq(HeartbeatLagMonitorAction.REMOVE_MONITOR),
        anyString());
    // Verify that unsubscribe completed pending subscribe without failing it.
    subscribeResult.get(3, TimeUnit.SECONDS);
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
        assertTrue(versionRef.get().isPartitionReadyToServe(0));
        assertFalse(versionRef.get().isPartitionReadyToServe(1));
      }
    });

    // Simulate unsubscribe from all partitions while future version ingestion is pending.
    subscribeResult = storeBackend.subscribe(ComplementSet.universalSet());
    storeBackend.unsubscribe(ComplementSet.universalSet());
    subscribeResult.get(3, TimeUnit.SECONDS);
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

    Version version3 = new VersionImpl(store.getName(), store.peekNextVersionNumber(), null, 3);
    store.addVersion(version3);
    backend.handleStoreChanged(storeBackend);

    versionMap.get(version3.kafkaTopicName()).completePartition(partition);
    store.setCurrentVersion(version3.getNumber());
    backend.handleStoreChanged(storeBackend);
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
        assertEquals(versionRef.get().getVersion().getNumber(), version3.getNumber());
      }
    });

    // Rollback happens here, expecting Da Vinci to switch back to v1.
    store.setCurrentVersion(1);
    backend.handleStoreChanged(storeBackend);
    versionMap.get(version1.kafkaTopicName()).completePartition(partition);
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
        assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
      }
      // Bootstrap checker thread can also modify the versionMap, adding wait-assert here to avoid NPE.
      assertNotNull(versionMap.get(version2.kafkaTopicName()));
    });
    versionMap.get(version2.kafkaTopicName()).completePartition(partition);

    store.setCurrentVersion(3);
    backend.handleStoreChanged(storeBackend);
    versionMap.get(version3.kafkaTopicName()).completePartition(partition);
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
        assertEquals(versionRef.get().getVersion().getNumber(), version3.getNumber());
      }
    });
  }

  @Test
  public void testSubscribeWithDelayedIngestionEnabled() throws Exception {
    // delayed ingestion is not enabled; no target regions are set
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(0));
    versionMap.get(version1.kafkaTopicName()).completePartition(0);
    subscribeResult.get(3, TimeUnit.SECONDS);
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }

    versionMap.get(version2.kafkaTopicName()).completePartition(0);
    store.setCurrentVersion(version2.getNumber());
    backend.handleStoreChanged(storeBackend);
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version2.getNumber());
    }

    // delayed ingestion is enabled, target region is the current region
    store.setTargetSwapRegion("dc-0");
    Version version3 = new VersionImpl(store.getName(), store.peekNextVersionNumber(), null, 15);
    store.addVersion(version3);
    backend.handleStoreChanged(storeBackend);

    store.setCurrentVersion(version3.getNumber());
    versionMap.get(version3.kafkaTopicName()).completePartition(0);
    backend.handleStoreChanged(storeBackend);
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version3.getNumber());
    }

    // delayed ingestion is enabled, target region is NOT the current region (dc-0).
    // targetSwapRegion must be set on the version (not just the store) for the check to apply.
    // Non-target DVC clients subscribe immediately but in a paused state; they resume once
    // targetRegionPromoted=true is set by DeferredVersionSwapService.
    KafkaStoreIngestionService ingestionService = backend.getIngestionService();
    StoreIngestionTask pausedTask = mock(StoreIngestionTask.class);
    Version version4 = new VersionImpl(store.getName(), store.peekNextVersionNumber(), null, 15);
    version4.setTargetSwapRegion("dc-1"); // dc-0 is not a target region
    store.addVersion(version4);
    store.setCurrentVersion(version4.getNumber());
    store.updateVersionStatus(version4.getNumber(), VersionStatus.ONLINE);
    when(ingestionService.getStoreIngestionTask(version4.kafkaTopicName())).thenReturn(pausedTask);
    when(pausedTask.isFutureSlotPaused()).thenReturn(true);
    backend.handleStoreChanged(storeBackend);

    // Non-target region must subscribe immediately (createPaused=true), not skip.
    assertTrue(
        versionMap.containsKey(version4.kafkaTopicName()),
        "Non-target region must subscribe immediately with createPaused=true");
    verify(ingestionBackend, times(1)).startConsumption(any(), eq(0), any(), any(), eq(true));
    // version4 is paused, so current version must still be version3.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version3.getNumber());
    }

    // Once targetRegionPromoted flips to true, maybeResumeDaVinciFutureVersion should resume ingestion.
    // Keep isFutureSlotPaused()=true so isPaused() returns true and resume() is actually invoked.
    store.setVersionTargetRegionPromoted(version4.getNumber(), true);
    backend.handleStoreChanged(storeBackend);

    verify(pausedTask, times(1)).resumeFromFutureSlotPause();
    versionMap.get(version4.kafkaTopicName()).completePartition(0);
    backend.handleStoreChanged(storeBackend);
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version4.getNumber());
    }
  }

  /**
   * A non-target-region DVC client must subscribe to a target-region push immediately but in a
   * paused state (createPaused=true). The version backend is present in the map, and
   * startConsumption is called with createPaused=true.
   */
  @Test
  public void testCreatesPausedSITInNonTargetRegion() throws Exception {
    // Subscribe to version1 as current, version2 becomes future.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(0));
    versionMap.get(version1.kafkaTopicName()).completePartition(0);
    subscribeResult.get(3, TimeUnit.SECONDS);
    versionMap.get(version2.kafkaTopicName()).completePartition(0);
    store.setCurrentVersion(version2.getNumber());
    backend.handleStoreChanged(storeBackend);

    // Now add version3 with targetSwapRegion="dc-1"; local region is "dc-0" (non-target).
    Version version3 = new VersionImpl(store.getName(), store.peekNextVersionNumber(), null, 15);
    version3.setTargetSwapRegion("dc-1");
    store.addVersion(version3);
    store.setCurrentVersion(version3.getNumber());
    backend.handleStoreChanged(storeBackend);

    // Must subscribe (paused) immediately — non-target region subscribes with createPaused=true.
    assertTrue(versionMap.containsKey(version3.kafkaTopicName()), "Non-target region must subscribe immediately");
    verify(ingestionBackend, times(1)).startConsumption(any(), eq(0), any(), any(), eq(true));
  }

  /**
   * A target-region DVC client must subscribe to a target-region push immediately and active
   * (createPaused=false).
   */
  @Test
  public void testCreatesActiveSITInTargetRegion() throws Exception {
    // Subscribe to version1 as current, version2 becomes future.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(0));
    versionMap.get(version1.kafkaTopicName()).completePartition(0);
    subscribeResult.get(3, TimeUnit.SECONDS);
    versionMap.get(version2.kafkaTopicName()).completePartition(0);
    store.setCurrentVersion(version2.getNumber());
    backend.handleStoreChanged(storeBackend);

    // Now add version3 with targetSwapRegion="dc-0"; local region IS "dc-0" (target region).
    Version version3 = new VersionImpl(store.getName(), store.peekNextVersionNumber(), null, 15);
    version3.setTargetSwapRegion("dc-0");
    store.addVersion(version3);
    store.setCurrentVersion(version3.getNumber());
    backend.handleStoreChanged(storeBackend);

    // Must subscribe active (not paused) — this IS the target region.
    assertTrue(versionMap.containsKey(version3.kafkaTopicName()), "Target region must subscribe");
    verify(ingestionBackend, never()).startConsumption(any(), anyInt(), any(), any(), eq(true));
  }

  /**
   * When targetRegionPromoted flips to true, {@link StoreBackend#maybeResumeDaVinciFutureVersion()}
   * must call {@link StoreIngestionTask#resumeFromFutureSlotPause()} on the paused future version.
   */
  @Test
  public void testResumePausedSITOnTargetPromotion() throws Exception {
    // Subscribe to version1 as current, version2 becomes future.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(0));
    versionMap.get(version1.kafkaTopicName()).completePartition(0);
    subscribeResult.get(3, TimeUnit.SECONDS);
    versionMap.get(version2.kafkaTopicName()).completePartition(0);
    store.setCurrentVersion(version2.getNumber());
    backend.handleStoreChanged(storeBackend);

    // Add version3 in non-target region — subscribed paused.
    KafkaStoreIngestionService ingestionService = backend.getIngestionService();
    StoreIngestionTask pausedTask = mock(StoreIngestionTask.class);
    Version version3 = new VersionImpl(store.getName(), store.peekNextVersionNumber(), null, 15);
    version3.setTargetSwapRegion("dc-1");
    store.addVersion(version3);
    store.setCurrentVersion(version3.getNumber());
    when(ingestionService.getStoreIngestionTask(version3.kafkaTopicName())).thenReturn(pausedTask);
    when(pausedTask.isFutureSlotPaused()).thenReturn(true);
    backend.handleStoreChanged(storeBackend);

    assertTrue(versionMap.containsKey(version3.kafkaTopicName()), "Non-target region must subscribe");
    verify(pausedTask, never()).resumeFromFutureSlotPause();

    // Flip targetRegionPromoted — maybeResumeDaVinciFutureVersion should resume the task.
    // Keep isFutureSlotPaused()=true so that isPaused() returns true and resume() is actually invoked.
    store.setVersionTargetRegionPromoted(version3.getNumber(), true);
    backend.handleStoreChanged(storeBackend);

    verify(pausedTask, times(1)).resumeFromFutureSlotPause();
  }

  /**
   * With {@code DAVINCI_PAUSED_SIT_ENABLED=false} (legacy mode) a non-target-region DVC client must
   * NOT subscribe to a target-region push version until that version reaches
   * {@link VersionStatus#ONLINE} in the local region.
   */
  @Test
  public void testLegacyNonTargetRegionSubscribesOnOnline() throws Exception {
    // Re-create storeBackend with paused-SIT disabled (legacy mode).
    VeniceProperties legacyConfig = new PropertyBuilder().put(ConfigKeys.CLUSTER_NAME, "test-cluster")
        .put(ConfigKeys.ZOOKEEPER_ADDRESS, "test-zookeeper")
        .put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, "test-kafka")
        .put(ConfigKeys.DATA_BASE_PATH, baseDataPath.getAbsolutePath())
        .put(ConfigKeys.LOCAL_REGION_NAME, "dc-0")
        .put(ConfigKeys.DAVINCI_PAUSED_SIT_ENABLED, "false")
        .build();
    when(backend.getConfigLoader()).thenReturn(new VeniceConfigLoader(legacyConfig));
    storeBackend = new StoreBackend(backend, store.getName());
    when(backend.getStoreOrThrow(store.getName())).thenReturn(storeBackend);

    // Subscribe to version1 (current), version2 becomes future.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(0));
    versionMap.get(version1.kafkaTopicName()).completePartition(0);
    subscribeResult.get(3, TimeUnit.SECONDS);
    versionMap.get(version2.kafkaTopicName()).completePartition(0);
    store.setCurrentVersion(version2.getNumber());
    backend.handleStoreChanged(storeBackend);

    // Add version3 in non-target region (dc-0 is NOT dc-1).
    Version version3 = new VersionImpl(store.getName(), store.peekNextVersionNumber(), null, 15);
    version3.setTargetSwapRegion("dc-1");
    store.addVersion(version3);
    store.setCurrentVersion(version3.getNumber());
    backend.handleStoreChanged(storeBackend);

    // Legacy: non-target region must NOT subscribe while version is not ONLINE.
    assertFalse(
        versionMap.containsKey(version3.kafkaTopicName()),
        "Legacy mode: non-target region must not subscribe before version is ONLINE");

    // Mark version3 ONLINE — legacy gate should now allow subscription.
    store.updateVersionStatus(version3.getNumber(), VersionStatus.ONLINE);
    backend.handleStoreChanged(storeBackend);

    assertTrue(
        versionMap.containsKey(version3.kafkaTopicName()),
        "Legacy mode: non-target region must subscribe once version is ONLINE");
    // Legacy mode uses createPaused=false.
    verify(ingestionBackend, never()).startConsumption(any(), anyInt(), any(), any(), eq(true));
  }

  /**
   * Verify that {@link StoreBackend#setDaVinciFutureVersion} keeps
   * {@link KafkaStoreIngestionService}'s future-slot registry in sync so the
   * current-version-bootstrapping speedup throttler can skip future-slot versions.
   *
   * <ul>
   *   <li>Assigning a future version → {@code markAsDaVinciFutureSlot} is called for that topic.
   *   <li>Promoting future to current (via {@code trySwapDaVinciCurrentVersion}) →
   *       {@code unmarkAsDaVinciFutureSlot} is called.
   * </ul>
   */
  @Test
  public void testDaVinciFutureSlotRegistryIsKeptInSync() throws Exception {
    com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService ingestionService = backend.getIngestionService();

    // Subscribe to version1 (becomes daVinciCurrentVersion). version1 is the existing current
    // version, so trySubscribeDaVinciFutureVersion picks version2 as the future slot.
    CompletableFuture<?> subscribeResult = storeBackend.subscribe(ComplementSet.of(0));
    versionMap.get(version1.kafkaTopicName()).completePartition(0);
    subscribeResult.get(3, TimeUnit.SECONDS);

    // version2 should now be marked as the future slot.
    verify(ingestionService, times(1)).markAsDaVinciFutureSlot(version2.kafkaTopicName());
    verify(ingestionService, never()).unmarkAsDaVinciFutureSlot(version2.kafkaTopicName());

    // Promote version2 (the future) → currentVersion. swapCurrentVersion calls
    // setDaVinciFutureVersion(null), which should unmark the topic.
    versionMap.get(version2.kafkaTopicName()).completePartition(0);
    store.setCurrentVersion(version2.getNumber());
    backend.handleStoreChanged(storeBackend);

    verify(ingestionService, times(1)).unmarkAsDaVinciFutureSlot(version2.kafkaTopicName());

    // Sanity: DVC is now serving version2.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version2.getNumber());
    }
  }
}
