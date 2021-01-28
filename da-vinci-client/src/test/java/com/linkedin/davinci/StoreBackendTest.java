package com.linkedin.davinci;

import com.linkedin.venice.ConfigKeys;
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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.AdditionalAnswers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class StoreBackendTest {
  Store store;
  DaVinciBackend backend;
  StoreBackend storeBackend;
  Map<String, VersionBackend> versionMap;

  @BeforeMethod
  void setup() {
    VeniceProperties backendConfig = new PropertyBuilder()
        .put(ConfigKeys.CLUSTER_NAME, "test-cluster")
        .put(ConfigKeys.ZOOKEEPER_ADDRESS, "test-zookeeper")
        .put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, "test-kafka")
        .put(ConfigKeys.KAFKA_ZK_ADDRESS, "test-kafka-zookeeper")
        .put(ConfigKeys.DATA_BASE_PATH, TestUtils.getUniqueTempPath())
        .build();

    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    doAnswer(answerVoid(Runnable::run)).when(executor).execute(any());

    backend = mock(DaVinciBackend.class);
    when(backend.getExecutor()).thenReturn(executor);
    when(backend.getConfigLoader()).thenReturn(new VeniceConfigLoader(backendConfig));
    when(backend.getMetricsRepository()).thenReturn(new MetricsRepository());
    when(backend.getStoreRepository()).thenReturn(mock(SubscriptionBasedReadOnlyStoreRepository.class));
    when(backend.getStorageService()).thenReturn(mock(StorageService.class));
    when(backend.getIngestionService()).thenReturn(mock(StoreIngestionService.class));
    when(backend.getVersionByTopicMap()).thenReturn(new HashMap<>());
    when(backend.getLatestVersion(anyString())).thenCallRealMethod();
    when(backend.getCurrentVersion(anyString())).thenCallRealMethod();

    store = new ZKStore("test-store", null, 0, PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_ALL_REPLICAS);
    when(backend.getStoreRepository().getStoreOrThrow(store.getName())).thenReturn(store);

    versionMap = backend.getVersionByTopicMap();
    storeBackend = new StoreBackend(backend, store.getName());
  }

  @Test
  void testFutureVersionFailure() throws Exception {
    // Expecting to subscribe to version1 and that version2 is a future version.
    Version version1 = new Version(store.getName(), store.peekNextVersion().getNumber(), null, 10);
    store.addVersion(version1);
    Version version2 = new Version(store.getName(), store.peekNextVersion().getNumber(), null, 5);
    store.addVersion(version2);
    store.setCurrentVersion(version1.getNumber());

    int partition = 1;
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.of(partition));
    versionMap.get(version1.kafkaTopicName()).completeSubPartition(partition);
    subscribeResult.get(0, TimeUnit.SECONDS);
    assertTrue(versionMap.containsKey(version2.kafkaTopicName()));

    // Simulate future version kill and removal from Venice.
    store.deleteVersion(version2.getNumber());
    storeBackend.deleteOldVersions();
    // Verify that corresponding Version Backend is deleted exactly once.
    assertFalse(versionMap.containsKey(version2.kafkaTopicName()));
    verify(backend.getStorageService(), times(1)).dropStorePartition(any(), eq(partition));

    // Simulate new version push and subsequent ingestion failure.
    Version version3 = new Version(store.getName(), store.peekNextVersion().getNumber(), null, 15);
    store.addVersion(version3);
    storeBackend.trySubscribeFutureVersion();
    versionMap.get(version3.kafkaTopicName()).completeSubPartitionExceptionally(partition, new Exception());
    // Verify that neither of the bad versions became current.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version1.getNumber());
    }

    // Simulate new version push and subsequent successful ingestion.
    Version version4 = new Version(store.getName(), store.peekNextVersion().getNumber(), null, 20);
    store.addVersion(version4);
    storeBackend.trySubscribeFutureVersion();
    versionMap.get(version4.kafkaTopicName()).completeSubPartition(partition);
    // Verify that successfully ingested version became current.
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      assertEquals(versionRef.get().getVersion().getNumber(), version4.getNumber());
    }
  }

  @Test
  void testSubscribeUnsubscribe() throws Exception {
    Version version1 = new Version(store.getName(), store.peekNextVersion().getNumber(), null, 2);
    store.addVersion(version1);

    // Simulate concurrent unsubscribe call while subscription is pending.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.universalSet());
    versionMap.get(version1.kafkaTopicName()).completeSubPartition(0);
    assertFalse(subscribeResult.isDone());
    storeBackend.unsubscribe(ComplementSet.of(1));

    // Verify that unsubscribe can complete pending subscription without failing it.
    subscribeResult.get(0, TimeUnit.SECONDS);
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      assertTrue(versionRef.get().isSubPartitionReadyToServe(0));
      assertFalse(versionRef.get().isSubPartitionReadyToServe(1));
    }

    // Simulate new version push and subsequent unsubscribe-all call.
    Version version2 = new Version(store.getName(), store.peekNextVersion().getNumber(), null, 5);
    store.addVersion(version2);
    storeBackend.trySubscribeFutureVersion();
    storeBackend.unsubscribe(ComplementSet.universalSet());

    // Verify that all versions were deleted because subscription set became empty.
    assertTrue(versionMap.isEmpty());
    verify(backend.getStorageService(), times(2)).dropStorePartition(any(), eq(0));
  }

  @Test
  void testSubscribeClose() {
    // Expecting to subscribe to version1 and that version2 is a future version.
    Version version1 = new Version(store.getName(), store.peekNextVersion().getNumber(), null, 10);
    store.addVersion(version1);
    Version version2 = new Version(store.getName(), store.peekNextVersion().getNumber(), null, 5);
    store.addVersion(version2);
    store.setCurrentVersion(version1.getNumber());

    // Simulate concurrent store deletion while subscription is pending.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.universalSet());
    storeBackend.close();
    // Verify that pending subscription fails when store is closed, but none of the versions gets deleted.
    assertThrows(() -> subscribeResult.getNow(null));
    verify(backend.getStorageService(), never()).dropStorePartition(any(), anyInt());
  }

  @Test
  void testSubscribeDelete() {
    // Expecting to subscribe to version1 and that version2 is a future version.
    Version version1 = new Version(store.getName(), store.peekNextVersion().getNumber(), null, 10);
    store.addVersion(version1);
    Version version2 = new Version(store.getName(), store.peekNextVersion().getNumber(), null, 5);
    store.addVersion(version2);
    store.setCurrentVersion(version1.getNumber());

    // Simulate concurrent store deletion while subscription is pending.
    CompletableFuture subscribeResult = storeBackend.subscribe(ComplementSet.universalSet());
    storeBackend.delete();
    // Verify that pending subscription fails when store is deleted and all versions are deleted exactly once.
    assertThrows(() -> subscribeResult.getNow(null));
    verify(backend.getStorageService(), times(2)).dropStorePartition(any(), eq(0));
  }
}
