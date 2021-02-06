package com.linkedin.davinci;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.davinci.storage.StorageService;

import io.tehuti.metrics.MetricsRepository;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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

import static org.mockito.AdditionalAnswers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class StoreBackendTest {
  Store store;
  Version version1;
  Version version2;
  File baseDataPath;
  DaVinciBackend backend;
  StoreBackend storeBackend;
  Map<String, VersionBackend> versionMap;
  MetricsRepository metricsRepository;

  @BeforeMethod
  void setup() {
    baseDataPath = TestUtils.getTempDataDirectory();
    VeniceProperties backendConfig = new PropertyBuilder()
        .put(ConfigKeys.CLUSTER_NAME, "test-cluster")
        .put(ConfigKeys.ZOOKEEPER_ADDRESS, "test-zookeeper")
        .put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, "test-kafka")
        .put(ConfigKeys.KAFKA_ZK_ADDRESS, "test-kafka-zookeeper")
        .put(ConfigKeys.DATA_BASE_PATH, baseDataPath.getAbsolutePath())
        .build();

    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    doAnswer(answerVoid(Runnable::run)).when(executor).execute(any());

    versionMap = new HashMap<>();
    metricsRepository = new MetricsRepository();
    backend = mock(DaVinciBackend.class);
    when(backend.getExecutor()).thenReturn(executor);
    when(backend.getConfigLoader()).thenReturn(new VeniceConfigLoader(backendConfig));
    when(backend.getMetricsRepository()).thenReturn(metricsRepository);
    when(backend.getStoreRepository()).thenReturn(mock(SubscriptionBasedReadOnlyStoreRepository.class));
    when(backend.getStorageService()).thenReturn(mock(StorageService.class));
    when(backend.getIngestionService()).thenReturn(mock(StoreIngestionService.class));
    when(backend.getVersionByTopicMap()).thenReturn(versionMap);
    when(backend.getLatestVersion(anyString(), anySet())).thenCallRealMethod();
    when(backend.getCurrentVersion(anyString(), anySet())).thenCallRealMethod();

    store = new ZKStore("test-store", null, 0, PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_ALL_REPLICAS);
    version1 = new Version(store.getName(), store.peekNextVersion().getNumber(), null, 5);
    store.addVersion(version1);
    version2 = new Version(store.getName(), store.peekNextVersion().getNumber(), null, 3);
    store.addVersion(version2);
    store.setCurrentVersion(version1.getNumber());
    when(backend.getStoreRepository().getStoreOrThrow(store.getName())).thenReturn(store);

    storeBackend = new StoreBackend(backend, store.getName());
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
    version1.setAge(Duration.ofHours(1));
    version2.setAge(Duration.ofMinutes(5));
    assertEquals(metricsRepository.getMetric("." + store.getName() + "--data_age_ms.Gauge").value(), Double.NaN);

    // Expecting to subscribe to version1 and that version2 is a future version.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(partition));
    versionMap.get(version1.kafkaTopicName()).completeSubPartition(partition);
    subscribeResult.get(0, TimeUnit.SECONDS);
    // Verify that subscribe selected the current version by default.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }

    assertEquals(metricsRepository.getMetric("." + store.getName() + "--current_version.Gauge").value(), (double) version1.getNumber());
    assertEquals(metricsRepository.getMetric("." + store.getName() + "--future_version.Gauge").value(), (double) version2.getNumber());
    assertTrue(Math.abs(metricsRepository.getMetric("." + store.getName() + "--data_age_ms.Gauge").value() - version1.getAge().toMillis()) <
                  TimeUnit.SECONDS.toMillis(1));

    // Simulate future version ingestion is complete.
    versionMap.get(version2.kafkaTopicName()).completeSubPartition(partition);
    // Verify that future version became current once ingestion is complete.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version2.getNumber());
    }

    assertEquals(metricsRepository.getMetric("." + store.getName() + "--current_version.Gauge").value(), (double) version2.getNumber());
    assertTrue(Math.abs(metricsRepository.getMetric("." + store.getName() + "--data_age_ms.Gauge").value() - version2.getAge().toMillis()) <
                   TimeUnit.SECONDS.toMillis(1));
  }

  @Test
  void testSubscribeSlowCurrentVersion() throws Exception {
    int partition = 0;
    // Expecting to subscribe to version1 and that version2 is a future version.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(partition));
    assertFalse(subscribeResult.isDone());
    // Verify that subscribe selected the current version by default.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }

    // Simulate future version ingestion is complete.
    versionMap.get(version2.kafkaTopicName()).completeSubPartition(partition);
    subscribeResult.get(0, TimeUnit.SECONDS);
    // Verify that future version became current once ingestion is complete.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version2.getNumber());
    }
  }

  @Test
  void testSubscribeWithoutCurrentVersion() throws Exception {
    int partition = 1;
    store.setCurrentVersion(Store.NON_EXISTING_VERSION);
    // Expecting to subscribe to the latest version (version2).
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(partition));
    versionMap.get(version2.kafkaTopicName()).completeSubPartition(partition);
    subscribeResult.get(0, TimeUnit.SECONDS);
    // Verify that subscribe selected the latest version as current.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version2.getNumber());
    }
  }

  @Test
  void testSubscribeBootstrapVersion() throws Exception {
    Version version3 = new Version(store.getName(), store.peekNextVersion().getNumber(), null, 15);
    store.addVersion(version3);
    store.setCurrentVersion(version2.getNumber());

    int partition = 2;
    // Expecting to subscribe to the specified version (version1), which is nether current nor latest.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(partition), Optional.of(version1));
    versionMap.get(version1.kafkaTopicName()).completeSubPartition(partition);
    subscribeResult.get(0, TimeUnit.SECONDS);
    // Verify that subscribe selected the specified version as current.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }

    // Simulate future version ingestion is complete.
    versionMap.get(version3.kafkaTopicName()).completeSubPartition(partition);
    // Verify that future version became current once ingestion is complete.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version3.getNumber());
    }
  }

  @Test
  void testFutureVersionFailure() throws Exception {
    int partition = 1;
    // Expecting to subscribe to version1 and that version2 is a future version.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(partition));
    versionMap.get(version1.kafkaTopicName()).completeSubPartition(partition);
    subscribeResult.get(0, TimeUnit.SECONDS);
    assertTrue(versionMap.containsKey(version2.kafkaTopicName()));

    // Simulate future version kill and removal from Venice.
    store.deleteVersion(version2.getNumber());
    storeBackend.deleteOldVersions();
    // Verify that corresponding Version Backend is deleted exactly once.
    assertFalse(versionMap.containsKey(version2.kafkaTopicName()));
    verify(backend.getStorageService(), times(1)).removeStorageEngine(eq(version2.kafkaTopicName()));

    // Simulate new version push and subsequent ingestion failure.
    Version version3 = new Version(store.getName(), store.peekNextVersion().getNumber(), null, 15);
    store.addVersion(version3);
    storeBackend.trySubscribeFutureVersion();

    // Simulate new version push while faulty future version is being ingested.
    Version version4 = new Version(store.getName(), store.peekNextVersion().getNumber(), null, 20);
    store.addVersion(version4);
    storeBackend.trySubscribeFutureVersion();

    versionMap.get(version3.kafkaTopicName()).completeSubPartitionExceptionally(partition, new Exception());
    // Verify that neither of the bad versions became current.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }

    versionMap.get(version4.kafkaTopicName()).completeSubPartition(partition);
    // Verify that successfully ingested version became current.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version4.getNumber());
    }

    // Simulate new version push and subsequent ingestion failure.
    Version version5 = new Version(store.getName(), store.peekNextVersion().getNumber(), null, 30);
    store.addVersion(version5);
    storeBackend.trySubscribeFutureVersion();
    versionMap.get(version5.kafkaTopicName()).completeSubPartitionExceptionally(partition, new Exception());
    // Verify that corresponding Version Backend is deleted exactly once.
    assertFalse(versionMap.containsKey(version5.kafkaTopicName()));
    verify(backend.getStorageService(), times(1)).removeStorageEngine(eq(version5.kafkaTopicName()));

    // Verify that faulty version will not be tried again.
    storeBackend.trySubscribeFutureVersion();
    assertFalse(versionMap.containsKey(version5.kafkaTopicName()));
  }

  @Test
  void testSubscribeUnsubscribe() throws Exception {
    // Simulate concurrent unsubscribe while subscribe is pending.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(0, 1));
    versionMap.get(version1.kafkaTopicName()).completeSubPartition(0);
    assertFalse(subscribeResult.isDone());
    storeBackend.unsubscribe(ComplementSet.of(1));
    // Verify that unsubscribe completed pending subscribe without failing it.
    subscribeResult.get(0, TimeUnit.SECONDS);
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      assertTrue(versionRef.get().isSubPartitionReadyToServe(0));
      assertFalse(versionRef.get().isSubPartitionReadyToServe(1));
    }

    // Simulate unsubscribe from all partitions while future version ingestion is pending.
    subscribeResult = storeBackend.subscribe(ComplementSet.universalSet());
    storeBackend.unsubscribe(ComplementSet.universalSet());
    subscribeResult.get(0, TimeUnit.SECONDS);
    // Verify that all versions were deleted because subscription set became empty.
    assertTrue(versionMap.isEmpty());
    assertEquals(FileUtils.sizeOfDirectory(baseDataPath), 0);
    verify(backend.getStorageService(), times(store.getVersions().size())).removeStorageEngine(any());
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
    verify(backend.getStorageService(), times(store.getVersions().size())).removeStorageEngine(any());
  }
}
